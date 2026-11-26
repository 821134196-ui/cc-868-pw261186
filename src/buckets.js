/**
 * 两级令牌桶：持续额度（sustained，容量小、恢复快）+ 突发额度（burst，容量大、恢复慢）。
 * 扣费顺序：先持续后突发；退款按原拆账比例退回各自桶。
 * 所有方法都可传入 now（毫秒）便于测试；默认 Date.now()。
 */
export class TieredBucket {
  constructor(quota, now = Date.now()) {
    this.capS = quota.sustained.capacity;
    this.rateS = quota.sustained.refillPerSec;
    this.capB = quota.burst.capacity;
    this.rateB = quota.burst.refillPerSec;
    this.sustained = this.capS;
    this.burst = this.capB;
    this.lastRefill = now;
  }

  /** 配额参数热更新：先结算到当前时刻，再换参数，水位按新容量截断。 */
  updateQuota(quota, now = Date.now()) {
    this._refill(now);
    this.capS = quota.sustained.capacity;
    this.rateS = quota.sustained.refillPerSec;
    this.capB = quota.burst.capacity;
    this.rateB = quota.burst.refillPerSec;
    this.sustained = Math.min(this.sustained, this.capS);
    this.burst = Math.min(this.burst, this.capB);
  }

  _refill(now) {
    const dt = Math.max(0, (now - this.lastRefill) / 1000);
    this.lastRefill = now;
    this.sustained = Math.min(this.capS, this.sustained + dt * this.rateS);
    this.burst = Math.min(this.capB, this.burst + dt * this.rateB);
  }

  available(now = Date.now()) {
    this._refill(now);
    return this.sustained + this.burst;
  }

  /**
   * 扣减 amount。成功返回拆账 { s, b }（s+b === amount），失败返回 null 且水位不变。
   */
  charge(amount, now = Date.now()) {
    this._refill(now);
    if (amount > this.sustained + this.burst + 1e-9) return null;
    const fromS = Math.min(this.sustained, amount);
    this.sustained -= fromS;
    this.burst -= amount - fromS;
    return { s: fromS, b: amount - fromS };
  }

  /** 按 charge 返回的拆账退回（按容量截断，多出的部分自然蒸发）。 */
  refund(split, now = Date.now()) {
    this._refill(now);
    this.sustained = Math.min(this.capS, this.sustained + split.s);
    this.burst = Math.min(this.capB, this.burst + split.b);
  }

  /** 估算凑齐 amount 还需多少秒（两级合计速率）；永不恢复时返回 Infinity。 */
  retryAfter(amount, now = Date.now()) {
    this._refill(now);
    const missing = amount - (this.sustained + this.burst);
    if (missing <= 0) return 0;
    const rate = this.rateS + this.rateB;
    if (rate <= 0) return Infinity;
    return missing / rate;
  }

  snapshot(now = Date.now()) {
    this._refill(now);
    return {
      sustained: round2(this.sustained),
      burst: round2(this.burst),
      sustainedCapacity: this.capS,
      burstCapacity: this.capB,
      sustainedRefillPerSec: this.rateS,
      burstRefillPerSec: this.rateB,
    };
  }
}

const round2 = (n) => Math.round(n * 100) / 100;
