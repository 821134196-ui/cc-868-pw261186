/**
 * 单个在途请求的计量器：记录每一笔扣费的拆账，支持对指定记录退款，
 * 结束时产出结算摘要。取消/后端提前拒绝时，已计量的部分就是“可观察的消耗”。
 */
export class RequestMeter {
  constructor(bucket) {
    this.bucket = bucket;
    this.records = []; // { s, b, amount, left }
    this.total = 0;    // 累计扣费
    this.refunded = 0; // 累计退费
    this.units = 0;    // 已计量的可观察单位（块数 / tick 数）
    this.startedAt = Date.now();
    this.closed = false;
  }

  /** 扣一笔费用，返回可退款记录；额度不足返回 null。 */
  charge(amount) {
    if (this.closed) return null;
    const split = this.bucket.charge(amount);
    if (!split) return null;
    const rec = { s: split.s, b: split.b, amount, left: amount };
    this.records.push(rec);
    this.total += amount;
    return rec;
  }

  /** 计量一个可观察单位（流式=一个块，慢请求=一个时间 tick）。 */
  chargeUnit(amount) {
    const rec = this.charge(amount);
    if (rec) this.units += 1;
    return rec;
  }

  /** 按记录退款（按原拆账比例退回持续/突发桶）。返回实际退的金额。 */
  refund(rec) {
    if (!rec || rec.left <= 0) return 0;
    const frac = rec.amount > 0 ? rec.left / rec.amount : 0;
    this.bucket.refund({ s: rec.s * frac, b: rec.b * frac });
    this.refunded += rec.left;
    const r = rec.left;
    rec.left = 0;
    return r;
  }

  /** 结算：关闭计量器并返回摘要。幂等之外的调用由网关保证只调一次。 */
  settle(reason) {
    this.closed = true;
    return {
      reason,
      total: round1(this.total),
      refunded: round1(this.refunded),
      net: round1(this.total - this.refunded),
      units: this.units,
      durationMs: Date.now() - this.startedAt,
    };
  }
}

const round1 = (n) => Math.round(n * 10) / 10;
