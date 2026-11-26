/**
 * 全局默认配置与对比实验预设。
 * config 在运行时可被 /api/config 修改（网关内生效，不改本文件）。
 */

export const DEFAULT_CONFIG = {
  gatewayPort: 8080,
  upstreams: {
    stream: 'http://127.0.0.1:9101',
    query: 'http://127.0.0.1:9102',
    slow: 'http://127.0.0.1:9103',
  },
  // 代价模型：不同请求类别不同计费方式
  costs: {
    stream: { admission: 5, perChunk: 1 }, // 准入费 + 每投递一个 NDJSON 块
    query: { admission: 2 },               // 轻量查询：固定费用
    slow: { admission: 3, perSecond: 4 },  // 准入费 + 按占用时长（每 250ms 计量一次）
  },
  // 每个租户两级令牌桶：持续额度（快恢复）+ 突发额度（深但慢恢复）
  quota: {
    sustained: { capacity: 60, refillPerSec: 6 },
    burst: { capacity: 120, refillPerSec: 1 },
  },
};

/** 调试页“场景预设”：用于比较不同代价下的拒绝与额度恢复。每个预设都是完整配置。 */
export const PRESETS = {
  balanced: {
    costs: {
      stream: { admission: 5, perChunk: 1 },
      query: { admission: 2 },
      slow: { admission: 3, perSecond: 4 },
    },
    quota: {
      sustained: { capacity: 60, refillPerSec: 6 },
      burst: { capacity: 120, refillPerSec: 1 },
    },
  },
  'cheap-stream': {
    costs: {
      stream: { admission: 1, perChunk: 0.2 },
      query: { admission: 1 },
      slow: { admission: 3, perSecond: 4 },
    },
    quota: {
      sustained: { capacity: 60, refillPerSec: 6 },
      burst: { capacity: 120, refillPerSec: 1 },
    },
  },
  'pricey-stream': {
    costs: {
      stream: { admission: 20, perChunk: 5 },
      query: { admission: 2 },
      slow: { admission: 3, perSecond: 4 },
    },
    quota: {
      sustained: { capacity: 60, refillPerSec: 6 },
      burst: { capacity: 120, refillPerSec: 1 },
    },
  },
  'shallow-burst': {
    costs: {
      stream: { admission: 5, perChunk: 1 },
      query: { admission: 2 },
      slow: { admission: 3, perSecond: 4 },
    },
    quota: {
      sustained: { capacity: 30, refillPerSec: 6 },
      burst: { capacity: 25, refillPerSec: 0.5 },
    },
  },
  'deep-burst': {
    costs: {
      stream: { admission: 5, perChunk: 1 },
      query: { admission: 2 },
      slow: { admission: 3, perSecond: 4 },
    },
    quota: {
      sustained: { capacity: 40, refillPerSec: 4 },
      burst: { capacity: 400, refillPerSec: 2 },
    },
  },
};
