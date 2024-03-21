const Redis = require("ioredis");

const redisConnection = new Redis({
  host: process.env.REDIS_HOST || "localhost",
  port: process.env.REDIS_PORT || 6379,
  maxRetriesPerRequest: null
  // any other configurations
});

redisConnection.setMaxListeners(0);
module.exports = redisConnection;
