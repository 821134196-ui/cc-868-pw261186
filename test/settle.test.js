import test from 'node:test';
import assert from 'node:assert/strict';
import { startBackends } from '../src/backends.js';
import { createGateway } from '../src/gateway.js';

/**
 * 端到端结算测试：真实 HTTP 经过网关 → 样例后端，
 * 验证取消、后端提前拒绝、中途熔断时的“按可观察消耗结算”。
 */

const COSTS = {
  stream: { admission: 5, perChunk: 1 },
  query: { admission: 2 },
  slow: { admission: 3, perSecond: 4 }, // 每 250ms tick = 1
};

function makeConfig(upstreams, quota) {
  return {
    upstreams,
    costs: structuredClone(COSTS),
    quota: quota ?? {
      sustained: { capacity: 1000, refillPerSec: 0 },
      burst: { capacity: 1000, refillPerSec: 0 },
    },
  };
}

async function setup(quota) {
  const backends = await startBackends({ stream: 0, query: 0, slow: 0 });
  const upstreams = {
    stream: `http://127.0.0.1:${backends.ports.stream}`,
    query: `http://127.0.0.1:${backends.ports.query}`,
    slow: `http://127.0.0.1:${backends.ports.slow}`,
  };
  const gw = createGateway({ config: makeConfig(upstreams, quota) });
  const port = await gw.listen(0);
  const base = `http://127.0.0.1:${port}`;
  return {
    gw, backends, base,
    async close() { await gw.close(); await backends.close(); },
    settled(tenant) {
      return gw.events.items.filter((e) => e.type === 'settled' && e.tenant === tenant);
    },
  };
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

test('流式完成：净费用 = 准入费 + 块数×单价', async () => {
  const ctx = await setup();
  try {
    const res = await fetch(`${ctx.base}/stream?tenant=t1&chunks=5&interval=20`);
    assert.equal(res.status, 200);
    const text = await res.text();
    assert.equal(text.trim().split('\n').length, 5);
    await sleep(50);
    const [s] = ctx.settled('t1');
    assert.equal(s.reason, 'completed');
    assert.equal(s.units, 5);
    assert.equal(s.net, 5 + 5 * 1);
    assert.equal(s.refunded, 0);
  } finally { await ctx.close(); }
});

test('客户端中途取消：只结算已投递的块', async () => {
  const ctx = await setup();
  try {
    const ctrl = new AbortController();
    const res = await fetch(`${ctx.base}/stream?tenant=t2&chunks=50&interval=40`, { signal: ctrl.signal });
    let got = 0;
    const reader = res.body.getReader();
    // 读到约 3 块后取消
    while (got < 3) {
      const { value } = await reader.read();
      got += Buffer.from(value).toString('utf8').split('\n').filter(Boolean).length;
    }
    ctrl.abort();
    await sleep(150);
    const [s] = ctx.settled('t2');
    assert.equal(s.reason, 'client-cancelled');
    assert.ok(s.units >= 2 && s.units <= 6, `units=${s.units} 应在 2..6`);
    assert.equal(s.net, 5 + s.units * 1);
    assert.ok(s.units < 50, '取消后不应继续计费');
  } finally { await ctx.close(); }
});

test('后端中途崩溃（failAfter）：按已观察到的块结算', async () => {
  const ctx = await setup();
  try {
    const res = await fetch(`${ctx.base}/stream?tenant=t3&chunks=20&interval=20&failAfter=3`);
    const text = await res.text();
    const lines = text.trim().split('\n').filter(Boolean);
    assert.equal(lines.filter((l) => !l.includes('gatewayError')).length, 3);
    await sleep(50);
    const [s] = ctx.settled('t3');
    assert.equal(s.reason, 'backend-aborted');
    assert.equal(s.units, 3);
    assert.equal(s.net, 5 + 3);
  } finally { await ctx.close(); }
});

test('后端产出前拒绝：准入费全额退还，净费用 0', async () => {
  const ctx = await setup();
  try {
    const res = await fetch(`${ctx.base}/stream?tenant=t4&reject=1`);
    assert.equal(res.status, 503);
    await res.text();
    await sleep(50);
    const [s] = ctx.settled('t4');
    assert.equal(s.reason, 'backend-rejected');
    assert.equal(s.net, 0);
    assert.equal(s.refunded, 5);
    // 租户额度应回到满水位
    const snap = ctx.gw.tenants.get('t4').bucket.snapshot();
    assert.equal(snap.sustained + snap.burst, 2000);
  } finally { await ctx.close(); }
});

test('准入额度不足：直接 429，不接触后端，不计费', async () => {
  const ctx = await setup({
    sustained: { capacity: 3, refillPerSec: 0 },
    burst: { capacity: 0, refillPerSec: 0 },
  });
  try {
    const res = await fetch(`${ctx.base}/stream?tenant=t5&chunks=5`);
    assert.equal(res.status, 429);
    const body = await res.json();
    assert.equal(body.error, 'quota_exceeded');
    assert.ok(res.headers.get('retry-after'));
    assert.equal(ctx.settled('t5').length, 0);
    assert.equal(ctx.gw.tenants.get('t5').counters.rejected, 1);
    assert.equal(ctx.gw.tenants.get('t5').counters.admitted, 0);
  } finally { await ctx.close(); }
});

test('流中途额度耗尽：熔断并附终止帧，已计量部分照计', async () => {
  const ctx = await setup({
    sustained: { capacity: 8, refillPerSec: 0 }, // 准入 5 + 约 3 块
    burst: { capacity: 0, refillPerSec: 0 },
  });
  try {
    const res = await fetch(`${ctx.base}/stream?tenant=t6&chunks=50&interval=20`);
    assert.equal(res.status, 200);
    const text = await res.text();
    assert.ok(text.includes('quota_exhausted'), '客户端应收到熔断终止帧');
    await sleep(50);
    const [s] = ctx.settled('t6');
    assert.equal(s.reason, 'quota-exhausted');
    assert.ok(s.units >= 2 && s.units <= 4, `units=${s.units}`);
    assert.equal(s.net, 5 + s.units);
    assert.ok(ctx.gw.tenants.get('t6').bucket.available() < 1);
  } finally { await ctx.close(); }
});

test('慢请求被取消：按已占用的时间 tick 结算', async () => {
  const ctx = await setup();
  try {
    const ctrl = new AbortController();
    const p = fetch(`${ctx.base}/slow?tenant=t7&ms=5000`, { signal: ctrl.signal });
    await sleep(700); // 约 2 个 tick（250ms/个）
    ctrl.abort();
    await p.catch(() => {});
    await sleep(100);
    const [s] = ctx.settled('t7');
    assert.equal(s.reason, 'client-cancelled');
    assert.ok(s.units >= 1 && s.units <= 4, `units=${s.units}`);
    assert.equal(s.net, 3 + s.units * 1);
  } finally { await ctx.close(); }
});

test('慢请求被后端拒绝：退准入费，已观察 tick 照计', async () => {
  const ctx = await setup();
  try {
    const res = await fetch(`${ctx.base}/slow?tenant=t8&ms=150&reject=1`);
    assert.equal(res.status, 503);
    await res.text();
    await sleep(50);
    const [s] = ctx.settled('t8');
    assert.equal(s.reason, 'backend-rejected');
    assert.equal(s.refunded, 3);
    assert.ok(s.net <= 1, `net=${s.net} 应 ≈0（至多 1 个 tick）`);
  } finally { await ctx.close(); }
});

test('轻量查询：固定费用；后端拒绝则全退', async () => {
  const ctx = await setup();
  try {
    const ok = await fetch(`${ctx.base}/query?tenant=t9&echo=a`);
    assert.equal(ok.status, 200);
    await ok.text();
    const no = await fetch(`${ctx.base}/query?tenant=t9&reject=1`);
    assert.equal(no.status, 503);
    await no.text();
    await sleep(50);
    const [s1, s2] = ctx.settled('t9');
    assert.equal(s1.net, 2);
    assert.equal(s2.net, 0);
    assert.equal(s2.refunded, 2);
  } finally { await ctx.close(); }
});

test('额度随时间恢复（持续快、突发慢）', async () => {
  const ctx = await setup({
    sustained: { capacity: 10, refillPerSec: 10 },
    burst: { capacity: 100, refillPerSec: 1 },
  });
  try {
    const b = ctx.gw.tenants.get('tx')?.bucket ?? null;
    // 通过一次查询建立租户并消耗
    const res = await fetch(`${ctx.base}/query?tenant=tx`);
    await res.text();
    const bucket = ctx.gw.tenants.get('tx').bucket;
    bucket.charge(bucket.available()); // 抽干
    assert.ok(bucket.available() < 1);
    await sleep(1050);
    const avail = bucket.available();
    assert.ok(avail > 8 && avail < 14, `1s 后应恢复约 10+1，实际 ${avail}`);
    assert.ok(bucket.snapshot().burst < 5, '突发恢复慢');
  } finally { await ctx.close(); }
});

test('多租户互相隔离', async () => {
  const ctx = await setup({
    sustained: { capacity: 6, refillPerSec: 0 },
    burst: { capacity: 0, refillPerSec: 0 },
  });
  try {
    // ta 的查询（2）后只剩 4，ta 的流式（准入 5）被拒
    const q = await fetch(`${ctx.base}/query?tenant=ta`);
    await q.text();
    const s1 = await fetch(`${ctx.base}/stream?tenant=ta&chunks=3`);
    assert.equal(s1.status, 429);
    // tb 是独立桶，不受影响
    const s2 = await fetch(`${ctx.base}/stream?tenant=tb&chunks=2&interval=20`);
    assert.equal(s2.status, 200);
    await s2.text();
  } finally { await ctx.close(); }
});
