const Redis = require('ioredis');

const redisOptions = process.env.REDIS_URL || process.env.REDIS_HOST
  ? (process.env.REDIS_URL || { host: process.env.REDIS_HOST, port: process.env.REDIS_PORT || 6379 })
  : undefined;

const redis = redisOptions ? new Redis(redisOptions) : new Redis();

const IDEMPOTENCY_PREFIX = 'idemp:';
const DEFAULT_TTL = parseInt(process.env.IDEMPOTENCY_TTL_SECONDS || '86400', 10); // 1 day

const isProcessed = async (commandId) => {
  if (!commandId) return false;
  try {
    const exists = await redis.exists(IDEMPOTENCY_PREFIX + commandId);
    return exists === 1;
  } catch (err) {
    console.error('[Idempotency Store] Redis check failed:', err.message);
    // Fail open: if Redis unavailable, avoid blocking processing
    return false;
  }
};

const markProcessed = async (commandId, ttlSeconds = DEFAULT_TTL) => {
  if (!commandId) return;
  try {
    await redis.set(IDEMPOTENCY_PREFIX + commandId, '1', 'EX', ttlSeconds);
  } catch (err) {
    console.error('[Idempotency Store] Redis set failed:', err.message);
  }
};

// Atomically claim a command id for processing. Returns true if this caller
// successfully claimed it (key did not exist before). Uses SET NX EX.
const tryClaim = async (commandId, ttlSeconds = DEFAULT_TTL) => {
  if (!commandId) return false;
  try {
    // ioredis returns 'OK' on success, null when NX prevents set
    const res = await redis.set(IDEMPOTENCY_PREFIX + commandId, '1', 'NX', 'EX', ttlSeconds);
    return res === 'OK';
  } catch (err) {
    console.error('[Idempotency Store] Redis tryClaim failed:', err.message);
    // Fail open: if Redis unavailable, avoid blocking processing
    return false;
  }
};

// Clear the processed mark (used when processing fails and we want to allow retries)
const clearProcessed = async (commandId) => {
  if (!commandId) return;
  try {
    await redis.del(IDEMPOTENCY_PREFIX + commandId);
  } catch (err) {
    console.error('[Idempotency Store] Redis del failed:', err.message);
  }
};

module.exports = { isProcessed, markProcessed, tryClaim, clearProcessed };
