import http from 'node:http';

/**
 * 本地样例后端，模拟三类上游服务：
 *  - stream :9101  NDJSON 流式接口，可模拟中途崩溃（failAfter）与直接拒绝（reject=1）
 *  - query  :9102  轻量查询，立即返回
 *  - slow   :9103  慢请求，占用 ms 毫秒后返回
 * 客户端（网关）断开时，后端立即停止工作并释放资源。
 */

const JSON_HEAD = { 'content-type': 'application/json; charset=utf-8' };

function clampInt(v, min, max, dflt) {
  const n = Number.parseInt(v ?? '', 10);
  if (Number.isNaN(n)) return dflt;
  return Math.min(max, Math.max(min, n));
}

function sendJson(res, code, obj) {
  res.writeHead(code, JSON_HEAD);
  res.end(JSON.stringify(obj));
}

function createStreamBackend() {
  return http.createServer((req, res) => {
    req.on('error', () => {});
    res.on('error', () => {});
    const url = new URL(req.url, 'http://localhost');
    if (url.pathname !== '/stream') return sendJson(res, 404, { error: 'not_found' });

    if (url.searchParams.get('reject') === '1') {
      // 后端在任何产出前拒绝（如自身过载）
      return sendJson(res, 503, { error: 'stream_backend_overloaded' });
    }

    const chunks = clampInt(url.searchParams.get('chunks'), 1, 1000, 20);
    const interval = clampInt(url.searchParams.get('interval'), 10, 5000, 100);
    const failAfter = clampInt(url.searchParams.get('failAfter'), 0, 100000, 0);

    res.writeHead(200, { 'content-type': 'application/x-ndjson', 'cache-control': 'no-store' });
    let i = 0;
    const timer = setInterval(() => {
      i += 1;
      try {
        res.write(JSON.stringify({ seq: i, of: chunks, ts: Date.now() }) + '\n');
      } catch {
        clearInterval(timer);
        return;
      }
      if (failAfter > 0 && i === failAfter) {
        // 模拟后端中途崩溃：等刚写的块冲刷出去后销毁连接，不发任何错误帧
        clearInterval(timer);
        setTimeout(() => req.socket.destroy(), 15).unref();
        return;
      }
      if (i >= chunks) {
        clearInterval(timer);
        res.end();
      }
    }, interval);
    // 网关取消/断开 → 立即停止产出
    req.on('close', () => clearInterval(timer));
  });
}

function createQueryBackend() {
  return http.createServer((req, res) => {
    req.on('error', () => {});
    res.on('error', () => {});
    const url = new URL(req.url, 'http://localhost');
    if (url.pathname !== '/query') return sendJson(res, 404, { error: 'not_found' });
    if (url.searchParams.get('reject') === '1') {
      return sendJson(res, 503, { error: 'query_backend_overloaded' });
    }
    sendJson(res, 200, {
      ok: true,
      ts: Date.now(),
      echo: url.searchParams.get('echo') ?? null,
      workMs: 1,
    });
  });
}

function createSlowBackend() {
  return http.createServer((req, res) => {
    req.on('error', () => {});
    res.on('error', () => {});
    const url = new URL(req.url, 'http://localhost');
    if (url.pathname !== '/slow') return sendJson(res, 404, { error: 'not_found' });

    const ms = clampInt(url.searchParams.get('ms'), 50, 30000, 4000);
    const timer = setTimeout(() => {
      if (url.searchParams.get('reject') === '1') {
        // 占用了少量时间后拒绝（例如后端排队后放弃）
        return sendJson(res, 503, { error: 'slow_backend_rejected', heldMs: ms });
      }
      sendJson(res, 200, { ok: true, tookMs: ms, ts: Date.now() });
    }, url.searchParams.get('reject') === '1' ? Math.min(ms, 150) : ms);
    // 网关取消 → 慢任务立即释放
    req.on('close', () => clearTimeout(timer));
  });
}

/**
 * 启动三个后端。ports 传 0 表示随机端口。
 * 返回 { ports: {stream, query, slow}, close() }。
 */
export async function startBackends(ports = { stream: 9101, query: 9102, slow: 9103 }) {
  const defs = [
    ['stream', createStreamBackend()],
    ['query', createQueryBackend()],
    ['slow', createSlowBackend()],
  ];
  const servers = {};
  const actual = {};
  await Promise.all(defs.map(([name, server]) => new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(ports[name] ?? 0, '127.0.0.1', () => {
      servers[name] = server;
      actual[name] = server.address().port;
      resolve();
    });
  })));
  return {
    ports: actual,
    close: () => Promise.all(Object.values(servers).map((s) => new Promise((r) => s.close(r)))),
  };
}
