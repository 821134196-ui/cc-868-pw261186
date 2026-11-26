/** 事件环形日志 + 计数器，供调试页轮询。 */

export class EventLog {
  constructor(limit = 300) {
    this.limit = limit;
    this.items = [];
    this.seq = 0;
  }

  push(type, fields = {}) {
    this.items.push({ seq: ++this.seq, t: new Date().toISOString(), type, ...fields });
    if (this.items.length > this.limit) {
      this.items.splice(0, this.items.length - this.limit);
    }
  }

  last(n) {
    return this.items.slice(-n);
  }

  clear() {
    this.items = [];
  }
}

export class Counters {
  constructor() {
    this.admitted = 0;        // 准入（通过准入扣费）的请求数
    this.rejected = 0;        // 准入即被 429 拒绝的请求数
    this.completed = 0;
    this.cancelled = 0;       // 客户端取消
    this.aborted = 0;         // 中途额度耗尽被熔断
    this.backendRejected = 0; // 后端提前拒绝/中断
    this.backendError = 0;    // 后端连接故障
    this.charged = 0;         // 累计扣费
    this.refunded = 0;        // 累计退费
  }

  add(other) {
    for (const k of Object.keys(this)) this[k] += other[k] || 0;
  }

  snapshot() {
    return { ...this, net: round1(this.charged - this.refunded) };
  }
}

const round1 = (n) => Math.round(n * 10) / 10;
