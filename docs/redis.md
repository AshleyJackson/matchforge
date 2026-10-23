# Redis usage

MatchForge uses Redis for ephemeral coordination state:

- participants
- waiting indexes
- claims
- idempotency records

Durable match records should remain in your application database through a
`PersistenceStore`.

## Upstash example

Install the client:

```bash
bun add @upstash/redis
```

Set:

```bash
UPSTASH_REDIS_REST_URL=...
UPSTASH_REDIS_REST_TOKEN=...
```

Then create the coordination store:

```ts
import { Redis } from "@upstash/redis";
import { Matcher, RedisCoordinationStore } from "matchforge";

const redis = new Redis({
  url: process.env.UPSTASH_REDIS_REST_URL!,
  token: process.env.UPSTASH_REDIS_REST_TOKEN!,
});

const matcher = new Matcher({
  store: new RedisCoordinationStore({
    redis,
    namespace: "my-app:matchmaking",
  }),
});
```

`RedisCoordinationStore` accepts any client that matches the exported
`RedisLike` interface. The constructor validates its options with
`RedisCoordinationStoreOptionsSchema` and throws if a required client method is
missing or a TTL is invalid.

Redis claims accept `fifo` or `random` selection and require a serializable
filter:

```ts
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
```

The Redis adapter evaluates filters inside a bounded Lua claim operation.
Arbitrary TypeScript compatibility, scoring, and relaxation callbacks are not
supported by Redis because executable functions cannot run inside Lua.

The claim scans at most `min(scanLimit, 100)` of the oldest waiting entries.
With `strategy: { type: "random" }`, the scan starts at an offset chosen by the
matcher's `random` function and wraps around inside that window. `fifo` starts
at the oldest entry.

`matched` and `none` results include two counters for capacity monitoring:

- `scanned`: candidates examined, excluding the requester and `excludeIds`
- `stale`: waiting entries removed because their participant record expired

## Namespaces and deployment

Use a unique namespace per application environment and tenant boundary. Keep
related keys in the same Redis Cluster hash slot when using Redis Cluster.
Partition large populations into coarse pools such as region, mode, or tenant.

The adapter applies TTLs to participant and claim records. Run
`matcher.recover()` from a scheduled worker to recover expired claims and
reconcile them with the durable persistence store.

The Redis adapter is horizontally safe for its supported claim contract, but
should still be load-tested with your pool sizes, filter complexity, and Redis
deployment.
