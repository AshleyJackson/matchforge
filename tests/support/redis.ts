import { RedisClient } from "bun";
import { Redis } from "@upstash/redis";
import type { RedisLike } from "../../index.ts";

/** Mirrors @upstash/redis automatic deserialization of replies. */
function parseRecursive(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(parseRecursive);
  if (typeof value !== "string") return value;
  try {
    const parsed: unknown = JSON.parse(value);
    if (typeof parsed === "number" && parsed.toString() !== value) return value;
    return parsed;
  } catch {
    return value;
  }
}

function serialize(value: unknown): string {
  return typeof value === "string" ? value : JSON.stringify(value);
}

export type IntegrationRedis = RedisLike & { close(): void };

/**
 * A `RedisLike` client over Bun's native Redis client that behaves like the
 * Upstash REST client, so one suite can run against local Redis and Upstash.
 */
export function createBunRedis(url: string): IntegrationRedis {
  const client = new RedisClient(url);
  const send = (command: string, args: unknown[]) =>
    client.send(command, args.map((arg) => String(arg)));
  return {
    get: async (key: string) => {
      const value = await send("GET", [key]);
      return value === null ? null : parseRecursive(value);
    },
    set: (key: string, value: unknown, options?: { ex?: number }) =>
      send("SET", [key, serialize(value), ...(options?.ex ? ["EX", options.ex] : [])]),
    del: (...keys: string[]) => send("DEL", keys),
    zadd: (key: string, entry: { score: number; member: string }) =>
      send("ZADD", [key, entry.score, entry.member]),
    zrem: (key: string, ...members: string[]) => send("ZREM", [key, ...members]),
    zscore: (key: string, member: string) => send("ZSCORE", [key, member]),
    zrange: async (key: string, start: number, stop: number) =>
      parseRecursive(await send("ZRANGE", [key, start, stop])),
    sadd: (key: string, ...members: string[]) => send("SADD", [key, ...members]),
    srem: (key: string, ...members: string[]) => send("SREM", [key, ...members]),
    smembers: async (key: string) => parseRecursive(await send("SMEMBERS", [key])),
    eval: async (script: string, keys: string[], args: string[]) =>
      parseRecursive(await send("EVAL", [script, keys.length, ...keys, ...args])),
    close: () => client.close(),
  };
}

export interface IntegrationBackend {
  name: string;
  create(): IntegrationRedis;
}

/** Real Redis backends configured through `REDIS_URL` and/or `UPSTASH_URL` + `UPSTASH_TOKEN`. */
export function integrationBackends(): IntegrationBackend[] {
  const backends: IntegrationBackend[] = [];
  const redisUrl = process.env.REDIS_URL;
  if (redisUrl) backends.push({ name: "Redis (REDIS_URL)", create: () => createBunRedis(redisUrl) });
  const url = process.env.UPSTASH_URL;
  const token = process.env.UPSTASH_TOKEN;
  if (url && token)
    backends.push({
      name: "Upstash",
      create: () => Object.assign(new Redis({ url, token }), { close: () => undefined }),
    });
  return backends;
}

const DELETE_NAMESPACE_SCRIPT = `
local keys = redis.call('keys', ARGV[1] .. '*')
for _, key in ipairs(keys) do redis.call('del', key) end
return #keys
`;

export async function deleteNamespace(redis: RedisLike, namespace: string): Promise<void> {
  await redis.eval(DELETE_NAMESPACE_SCRIPT, [], [`${namespace}:`]);
}
