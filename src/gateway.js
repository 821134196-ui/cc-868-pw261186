import http from 'node:http';
import { readFile } from 'node:fs/promises';
import { TieredBucket } from './buckets.js';
import { RequestMeter } from './meter.js';
import { EventLog, Counters } from './events.js';
import { PRESETS } from './config.js';

const DEBUG_HTML = new URL('../public/debug.html', import.meta.url);
const TICK_MS = 250; // 慢请求计量粒度

const JSON_HEAD = { 'content-type': 'application/json; charset=utf-8' };
const round1 = (n) => Math.round(n * 10) / 10;
const finite1 = (n) => (Number.isFinite(n) ? round1(n) : null);

/**
 * 请求转发网关。
 * 结算策略（按可观察的消耗）：
 *  - 准入费在转发前收取；额度不足直接 429，不产生任何费用。
 *  - 流式：每向上游观察到一行（块）计一次费；慢请求：每 250ms 计一次费；查询：仅准入费。
 *  - 客户端取消：已计量的费用即为消耗，结算即止，不多收不少收。
 *  - 后端在产出任何内容前拒绝（错误状态/连接失败）：准入费全额退还，净费用≈0。
 *  - 后端中途断开：已观察到的块/tick 与准入费照计（工作已发生）。
 *  - 在途期间额度耗尽：熔断该请求，已计量费用照计。
 */
export function createGateway({ config } = {}) {
  const events = new EventLog(300);
  const tenants = new Map(); // id -> { bucket, counters }
  let inFlight = 0;

  const tenantOf = (id) => {
    let t = tenants.get(id);
    if (!t) {
      t = { bucket: new TieredBucket(config.quota), counters: new Counters() };
      tenants.set(id, t);
      pushEvent('tenant-created', { tenant: id });
    }
    return t;
  };

  function pushEvent(type, fields, tid) {
    const t = tid ? tenants.get(tid) : null;
    events.push(type, { ...fields, ...(tid ? { tenant: tid } : {}), ...(t ? { levels: t.bucket.snapshot() } : {}) });
  }

  const log = (tid, type, fields) => pushEvent(type, fields, tid);

  // ---------- 工具 ----------

  function sendJson(res, code, obj, headers = {}) {
    res.writeHead(code, { ...JSON_HEAD, ...headers });
    res.end(JSON.stringify(obj));
  }

  function tenantId(req, url) {
    const raw = url.searchParams.get('tenant') || req.headers['x-tenant-id'] || 'anon';
    return String(raw).slice(0, 64) || 'anon';
  }

  function forwardParams(url, keys) {
    const p = new URLSearchParams();
    for (const k of keys) if (url.searchParams.has(k)) p.set(k, url.searchParams.get(k));
    const s = p.toString();
    return s ? `?${s}` : '';
  }

  function reject429(res, t, tid, kind, needed) {
    const ra = t.bucket.retryAfter(needed);
    t.counters.rejected += 1;
    log(tid, 'rejected', { kind, needed: round1(needed), available: round1(t.bucket.available()), retryAfterSec: finite1(ra) });
    sendJson(res, 429, { error: 'quota_exceeded', needed: round1(needed), retryAfterSec: finite1(ra) },
      { 'retry-after': String(Math.ceil(Number.isFinite(ra) ? ra : 60)) });
  }

  function bumpCounters(c, reason, summary) {
    c.charged += summary.total;
    c.refunded += summary.refunded;
    if (reason === 'completed') c.completed += 1;
    else if (reason === 'client-cancelled') c.cancelled += 1;
    else if (reason === 'quota-exhausted') c.aborted += 1;
    else if (reason === 'backend-rejected' || reason === 'backend-aborted') c.backendRejected += 1;
    else if (reason === 'backend-error') c.backendError += 1;
  }

  /** 构造 finalize：所有终止路径（完成/取消/熔断/后端异常）都汇入这里，只结算一次。 */
  function makeFinalizer(t, tid, kind, meter, cleanup) {
    let done = false;
    return (reason) => {
      if (done) return;
      done = true;
      inFlight -= 1;
      if (cleanup) cleanup();
      const summary = meter.settle(reason);
      bumpCounters(t.counters, reason, summary);
      log(tid, 'settled', { kind, ...summary });
    };
  }

  function admit(t, tid, kind, cost) {
    const meter = new RequestMeter(t.bucket);
    const rec = meter.charge(cost);
    if (!rec) return { meter: null, admission: null };
    inFlight += 1;
    t.counters.admitted += 1;
    log(tid, 'admitted', { kind, charged: round1(cost) });
    return { meter, admission: rec };
  }

  const safeEnd = (res, tail) => {
    if (res.destroyed || !res.writable) return;
    try {
      if (tail) res.write(tail);
      res.end();
    } catch { /* 客户端已断开 */ }
  };

  // ---------- 三类请求的转发处理 ----------

  function handleStream(req, res, url) {
    const tid = tenantId(req, url);
    const t = tenantOf(tid);
    const costs = config.costs.stream;
    const { meter, admission } = admit(t, tid, 'stream', costs.admission);
    if (!meter) return reject429(res, t, tid, 'stream', costs.admission);

    let gotBytes = false;
    let upstreamEnded = false;
    const finalize = makeFinalizer(t, tid, 'stream', meter);

    const upReq = http.get(config.upstreams.stream + '/stream' + forwardParams(url, ['chunks', 'interval', 'failAfter', 'reject']), (upRes) => {
      if (upRes.statusCode !== 200) {
        // 后端在产出前拒绝 → 准入费全额退还，净费用 0
        meter.refund(admission);
        log(tid, 'backend-rejected', { kind: 'stream', status: upRes.statusCode, refunded: round1(costs.admission) });
        res.writeHead(upRes.statusCode, JSON_HEAD);
        upRes.pipe(res);
        upRes.on('end', () => finalize('backend-rejected'));
        upRes.resume();
        return;
      }
      res.writeHead(200, { 'content-type': 'application/x-ndjson', 'cache-control': 'no-store' });
      upRes.setEncoding('utf8');
      let pending = '';
      upRes.on('data', (chunk) => {
        gotBytes = true;
        pending += chunk;
        let idx;
        let paid = true;
        while ((idx = pending.indexOf('\n')) >= 0) {
          pending = pending.slice(idx + 1);
          if (!meter.chargeUnit(costs.perChunk)) { paid = false; break; }
        }
        if (!paid) {
          // 流中途额度耗尽 → 熔断：给客户端一个可识别的终止帧
          log(tid, 'aborted', { kind: 'stream', cause: 'quota-exhausted', units: meter.units });
          finalize('quota-exhausted');
          upReq.destroy();
          safeEnd(res, JSON.stringify({ gatewayError: 'quota_exhausted', charged: round1(meter.total) }) + '\n');
          return;
        }
        if (!res.write(chunk)) {
          upRes.pause();
          res.once('drain', () => upRes.resume());
        }
      });
      upRes.on('end', () => {
        upstreamEnded = true;
        if (pending.trim()) meter.chargeUnit(costs.perChunk); // 末尾不足一行的产出也算可观察消耗
        safeEnd(res);
        finalize('completed');
      });
      upRes.on('close', () => {
        if (!upstreamEnded) {
          // 后端中途崩溃：已观察到的块照计
          finalize('backend-aborted');
          safeEnd(res, JSON.stringify({ gatewayError: 'upstream_aborted', charged: round1(meter.total) }) + '\n');
        }
      });
    });
    upReq.on('error', (err) => {
      if (!gotBytes) {
        meter.refund(admission);
        log(tid, 'backend-error', { kind: 'stream', message: err.code || String(err), refunded: round1(costs.admission) });
        if (!res.headersSent) res.writeHead(502, JSON_HEAD);
        safeEnd(res, JSON.stringify({ error: 'upstream_error' }));
        finalize('backend-error');
      } else {
        finalize('backend-aborted');
        safeEnd(res);
      }
    });
    res.on('close', () => {
      if (!res.writableFinished) {
        // 客户端取消：已计量部分即消耗，结算即止
        finalize('client-cancelled');
        upReq.destroy();
      }
    });
  }

  function handleSlow(req, res, url) {
    const tid = tenantId(req, url);
    const t = tenantOf(tid);
    const costs = config.costs.slow;
    const { meter, admission } = admit(t, tid, 'slow', costs.admission);
    if (!meter) return reject429(res, t, tid, 'slow', costs.admission);

    const tickCost = (costs.perSecond * TICK_MS) / 1000;
    let gotResponse = false;
    let tickTimer = null;
    const finalize = makeFinalizer(t, tid, 'slow', meter, () => tickTimer && clearInterval(tickTimer));

    const upReq = http.get(config.upstreams.slow + '/slow' + forwardParams(url, ['ms', 'reject']), (upRes) => {
      gotResponse = true;
      if (tickTimer) clearInterval(tickTimer);
      if (upRes.statusCode !== 200) {
        // 后端拒绝：退准入费；已观察到的占用 tick 照计
        meter.refund(admission);
        log(tid, 'backend-rejected', { kind: 'slow', status: upRes.statusCode, refunded: round1(costs.admission), keptTicks: meter.units });
        res.writeHead(upRes.statusCode, JSON_HEAD);
        upRes.pipe(res);
        upRes.on('end', () => finalize('backend-rejected'));
        upRes.resume();
        return;
      }
      res.writeHead(200, JSON_HEAD);
      upRes.pipe(res);
      upRes.on('end', () => finalize('completed'));
    });

    // 按占用时长计量：每个 tick 一次，付不起就熔断
    tickTimer = setInterval(() => {
      if (!meter.chargeUnit(tickCost)) {
        log(tid, 'aborted', { kind: 'slow', cause: 'quota-exhausted', units: meter.units });
        finalize('quota-exhausted');
        upReq.destroy();
        if (!res.headersSent) res.writeHead(429, JSON_HEAD);
        safeEnd(res, JSON.stringify({ error: 'quota_exhausted_mid_request', charged: round1(meter.total) }));
      }
    }, TICK_MS);

    upReq.on('error', (err) => {
      if (!gotResponse) {
        meter.refund(admission);
        log(tid, 'backend-error', { kind: 'slow', message: err.code || String(err), refunded: round1(costs.admission) });
        if (!res.headersSent) res.writeHead(502, JSON_HEAD);
        safeEnd(res, JSON.stringify({ error: 'upstream_error' }));
        finalize('backend-error');
      } else {
        finalize('backend-aborted');
        safeEnd(res);
      }
    });
    res.on('close', () => {
      if (!res.writableFinished) {
        finalize('client-cancelled');
        upReq.destroy();
      }
    });
  }

  function handleQuery(req, res, url) {
    const tid = tenantId(req, url);
    const t = tenantOf(tid);
    const costs = config.costs.query;
    const { meter, admission } = admit(t, tid, 'query', costs.admission);
    if (!meter) return reject429(res, t, tid, 'query', costs.admission);

    let gotResponse = false;
    const finalize = makeFinalizer(t, tid, 'query', meter);

    const upReq = http.get(config.upstreams.query + '/query' + forwardParams(url, ['echo', 'reject']), (upRes) => {
      gotResponse = true;
      if (upRes.statusCode !== 200) {
        meter.refund(admission);
        log(tid, 'backend-rejected', { kind: 'query', status: upRes.statusCode, refunded: round1(costs.admission) });
        res.writeHead(upRes.statusCode, JSON_HEAD);
        upRes.pipe(res);
        upRes.on('end', () => finalize('backend-rejected'));
        upRes.resume();
        return;
      }
      res.writeHead(200, JSON_HEAD);
      upRes.pipe(res);
      upRes.on('end', () => finalize('completed'));
    });
    upReq.on('error', (err) => {
      if (!gotResponse) {
        meter.refund(admission);
        log(tid, 'backend-error', { kind: 'query', message: err.code || String(err), refunded: round1(costs.admission) });
        if (!res.headersSent) res.writeHead(502, JSON_HEAD);
        safeEnd(res, JSON.stringify({ error: 'upstream_error' }));
        finalize('backend-error');
      } else {
        finalize('backend-aborted');
        safeEnd(res);
      }
    });
    res.on('close', () => {
      if (!res.writableFinished) {
        finalize('client-cancelled');
        upReq.destroy();
      }
    });
  }

  // ---------- 管理 API ----------

  function stateSnapshot() {
    const tenantsObj = {};
    const totals = new Counters();
    for (const [id, t] of tenants) {
      tenantsObj[id] = { bucket: t.bucket.snapshot(), counters: t.counters.snapshot() };
      totals.add(t.counters);
    }
    return {
      now: Date.now(),
      config: { costs: config.costs, quota: config.quota },
      tenants: tenantsObj,
      totals: totals.snapshot(),
      inFlight,
      events: events.last(150),
    };
  }

  const num = (v) => {
    const n = Number(v);
    return Number.isFinite(n) && n >= 0 ? n : null;
  };

  function mergeSection(target, patch, keys) {
    for (const k of keys) {
      if (patch && patch[k] !== undefined) {
        const n = num(patch[k]);
        if (n !== null) target[k] = n;
      }
    }
  }

  function applyConfig(res, body) {
    let parsed;
    try {
      parsed = JSON.parse(body || '{}');
    } catch {
      return sendJson(res, 400, { error: 'bad_json' });
    }
    if (parsed.preset !== undefined) {
      const p = PRESETS[parsed.preset];
      if (!p) return sendJson(res, 400, { error: 'unknown_preset', presets: Object.keys(PRESETS) });
      config.costs = structuredClone(p.costs);
      config.quota = structuredClone(p.quota);
    } else {
      if (parsed.costs) {
        mergeSection(config.costs.stream, parsed.costs.stream, ['admission', 'perChunk']);
        mergeSection(config.costs.query, parsed.costs.query, ['admission']);
        mergeSection(config.costs.slow, parsed.costs.slow, ['admission', 'perSecond']);
      }
      if (parsed.quota) {
        mergeSection(config.quota.sustained, parsed.quota.sustained, ['capacity', 'refillPerSec']);
        mergeSection(config.quota.burst, parsed.quota.burst, ['capacity', 'refillPerSec']);
      }
    }
    for (const t of tenants.values()) t.bucket.updateQuota(config.quota);
    pushEvent('config', { detail: parsed.preset ? `preset:${parsed.preset}` : 'updated' });
    sendJson(res, 200, { ok: true, config: { costs: config.costs, quota: config.quota } });
  }

  function resetAll(res) {
    tenants.clear();
    events.clear();
    pushEvent('reset', {});
    sendJson(res, 200, { ok: true });
  }

  function readBody(req, cb) {
    let body = '';
    req.on('data', (c) => {
      body += c;
      if (body.length > 1e5) req.destroy();
    });
    req.on('end', () => cb(body));
    req.on('error', () => {});
  }

  // ---------- 路由 ----------

  const server = http.createServer((req, res) => {
    const url = new URL(req.url, 'http://gateway');
    const path = url.pathname;
    try {
      if (path === '/stream') return handleStream(req, res, url);
      if (path === '/query') return handleQuery(req, res, url);
      if (path === '/slow') return handleSlow(req, res, url);
      if (path === '/api/state') return sendJson(res, 200, stateSnapshot());
      if (path === '/api/config' && req.method === 'POST') return readBody(req, (b) => applyConfig(res, b));
      if (path === '/api/reset' && req.method === 'POST') return resetAll(res);
      if (path === '/debug') {
        return readFile(DEBUG_HTML).then(
          (html) => { res.writeHead(200, { 'content-type': 'text/html; charset=utf-8' }); res.end(html); },
          () => sendJson(res, 500, { error: 'debug_page_missing' }),
        );
      }
      if (path === '/') {
        res.writeHead(302, { location: '/debug' });
        return res.end();
      }
      sendJson(res, 404, { error: 'not_found' });
    } catch (err) {
      events.push('gateway-error', { message: String(err && err.message || err) });
      if (!res.headersSent) res.writeHead(500, JSON_HEAD);
      safeEnd(res, JSON.stringify({ error: 'gateway_error' }));
    }
  });

  return {
    server,
    tenants,
    events,
    config,
    stateSnapshot,
    listen: (port = 8080, host = '127.0.0.1') => new Promise((resolve, reject) => {
      server.once('error', reject);
      server.listen(port, host, () => resolve(server.address().port));
    }),
    port: () => server.address()?.port,
    close: () => new Promise((r) => server.close(r)),
  };
}
