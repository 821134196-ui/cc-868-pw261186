import { startBackends } from './backends.js';
import { createGateway } from './gateway.js';
import { DEFAULT_CONFIG } from './config.js';

const backends = await startBackends();
const gw = createGateway({ config: structuredClone(DEFAULT_CONFIG) });
const port = await gw.listen(Number(process.env.PORT || DEFAULT_CONFIG.gatewayPort));

console.log(`网关调试页:  http://127.0.0.1:${port}/debug`);
console.log(`转发入口:    /stream  /query  /slow   （用 ?tenant=xxx 或 X-Tenant-Id 指定租户）`);
console.log(`样例后端:    stream=:${backends.ports.stream}  query=:${backends.ports.query}  slow=:${backends.ports.slow}`);

process.on('SIGINT', async () => {
  await gw.close();
  await backends.close();
  process.exit(0);
});
