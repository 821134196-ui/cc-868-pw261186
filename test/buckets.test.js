import test from 'node:test';
import assert from 'node:assert/strict';
import { TieredBucket } from '../src/buckets.js';
import { RequestMeter } from '../src/meter.js';

const QUOTA = {
  sustained: { capacity: 10, refillPerSec: 5 },
  burst: { capacity: 20, refillPerSec: 1 },
};

test('扣费先走持续额度，再动用突发额度', () => {
  const b = new TieredBucket(QUOTA, 1000);
  const s1 = b.charge(6, 1000);
  assert.deepEqual(s1, { s: 6, b: 0 });
  const s2 = b.charge(8, 1000);
  assert.deepEqual(s2, { s: 4, b: 4 });
  assert.equal(Math.round(b.available(1000)), 16);
});

test('额度不足时拒绝且不产生部分扣减', () => {
  const b = new TieredBucket(QUOTA, 1000);
  assert.equal(b.charge(31, 1000), null);
  assert.equal(Math.round(b.available(1000)), 30);
});

test('按速率恢复且不超过容量', () => {
  const b = new TieredBucket(QUOTA, 0);
  b.charge(30, 0);
  assert.equal(Math.round(b.available(0)), 0);
  // 1 秒后：持续 +5，突发 +1
  assert.equal(Math.round(b.available(1000)), 6);
  // 100 秒后：都顶到容量
  assert.equal(Math.round(b.available(101000)), 30);
});

test('退款按原拆账退回各自桶', () => {
  const b = new TieredBucket(QUOTA, 0);
  const split = b.charge(15, 0); // { s:10, b:5 }
  b.refund(split, 0);
  assert.equal(Math.round(b.available(0)), 30);
  const snap = b.snapshot(0);
  assert.equal(snap.sustained, 10);
  assert.equal(snap.burst, 20);
});

test('retryAfter 估算凑齐费用的等待时间', () => {
  const b = new TieredBucket(QUOTA, 0);
  b.charge(30, 0);
  // 缺 12，合计速率 6/s → 2s
  assert.equal(b.retryAfter(12, 0), 2);
  assert.equal(b.retryAfter(0, 0), 0);
});

test('配额热更新后水位按新容量截断', () => {
  const b = new TieredBucket(QUOTA, 0);
  b.updateQuota({ sustained: { capacity: 5, refillPerSec: 5 }, burst: { capacity: 8, refillPerSec: 1 } }, 0);
  const snap = b.snapshot(0);
  assert.equal(snap.sustained, 5);
  assert.equal(snap.burst, 8);
});

test('计量器：结算摘要反映可观察消耗与退费', () => {
  const b = new TieredBucket(QUOTA, 0);
  const m = new RequestMeter(b);
  const adm = m.charge(5);
  m.chargeUnit(1);
  m.chargeUnit(1);
  m.refund(adm); // 后端产出前拒绝 → 退准入费
  const s = m.settle('backend-rejected');
  assert.equal(s.total, 7);
  assert.equal(s.refunded, 5);
  assert.equal(s.net, 2);
  assert.equal(s.units, 2);
});
