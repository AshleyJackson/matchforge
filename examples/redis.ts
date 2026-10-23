import { Redis } from "@upstash/redis";
import { Matcher, RedisCoordinationStore } from "../index.ts";

const redis = new Redis({
  url: process.env.UPSTASH_REDIS_REST_URL!,
  token: process.env.UPSTASH_REDIS_REST_TOKEN!,
});

const matcher = new Matcher({
  store: new RedisCoordinationStore({
    redis,
    namespace: "example:matchforge",
  }),
});

await matcher.register({
  id: "alice",
  preferences: { interests: ["music", "games"] },
});
await matcher.register({
  id: "bob",
  preferences: { interests: ["games"] },
});

await matcher.enqueue("alice", ["chat"]);
await matcher.enqueue("bob", ["chat"]);

const result = await matcher.findMatch({
  requesterId: "alice",
  poolKey: "chat",
  filter: {
    op: "overlaps",
    left: { source: "requester", path: ["interests"] },
    right: { source: "candidate", path: ["interests"] },
  },
});

console.log(result);
