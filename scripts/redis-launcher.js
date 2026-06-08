#!/usr/bin/env node
/**
 * redis-launcher.js
 * Starts a real Redis 7 server via redis-memory-server npm package.
 * No Homebrew or Docker required.
 */

const { RedisMemoryServer } = require('redis-memory-server');

const PORT = parseInt(process.env.REDIS_PORT || '6379', 10);

async function main() {
  console.log(`[Redis] Starting embedded Redis on port ${PORT}...`);

  const redisServer = new RedisMemoryServer({
    instance: {
      port: PORT,
      args: ['--save', '', '--appendonly', 'no'],
    },
  });

  const host = await redisServer.getHost();
  const port = await redisServer.getPort();

  console.log(`[Redis] ✅ Redis ready at ${host}:${port}`);
  console.log(`[Redis] Test: redis-cli -p ${port} ping`);

  // Keep process alive
  process.on('SIGINT', async () => {
    console.log('[Redis] Shutting down Redis...');
    await redisServer.stop();
    process.exit(0);
  });

  process.on('SIGTERM', async () => {
    await redisServer.stop();
    process.exit(0);
  });
}

main().catch((err) => {
  console.error('[Redis] Failed to start:', err.message);
  process.exit(1);
});
