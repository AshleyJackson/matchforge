# MatchForge

Framework-agnostic TypeScript matchmaking primitives for correct pair matching.

## What it does

MatchForge registers participants, places them into pools, and safely reserves
pairs under concurrent requests. It supports in-memory coordination for local
development and Redis/Lua coordination for distributed FIFO matching.

The package provides:

- compatibility functions and scoring for in-process matching
- serializable filters for Redis/Lua matching
- atomic claims, commits, rollbacks, and expiry recovery
- Zod validation for runtime inputs
- pluggable durable match persistence

## Quick start

```ts
import { InMemoryCoordinationStore, Matcher } from "./index.ts";

const matcher = new Matcher({
  store: new InMemoryCoordinationStore(),
});

await matcher.register({ id: "alice", preferences: { language: "en" } });
await matcher.register({ id: "bob", preferences: { language: "en" } });
await matcher.enqueue("alice", ["chat"]);
await matcher.enqueue("bob", ["chat"]);

const result = await matcher.findMatch({
  requesterId: "alice",
  poolKey: "chat",
  compatibility: (a, b) => a.preferences.language === b.preferences.language,
});
```

## Redis coordination

Use Redis instead of the in-memory store for distributed deployments:

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

The store accepts any client that matches the exported `RedisLike` interface,
so no cast is needed. Options are validated when the store is constructed.

Redis claims support `fifo` and `random` selection and require serializable
filters. `random` starts the bounded scan at a random offset inside the scan
window. `matched` and `none` results report `scanned` (candidates examined) and
`stale` (waiting entries removed because their participant record was gone).

`zod` is a peer dependency (`^4.0.0`), so schemas exported by MatchForge share
your application's zod instance.

For distributed Redis claims, use a serializable filter:

```ts
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

Filters support `all`, `any`, `not`, `overlaps`, `equals`, `in`, `contains`,
and `is-empty`.

Redis filters are domain-neutral. They use JSON paths and do not assume whether
a preference represents a language, game attribute, interest, or support field.

## Development

```bash
bun test
bun run build
bun run lint
bunx tsc --noEmit
```

When `UPSTASH_URL` and `UPSTASH_TOKEN` are present, `bun test` also runs the
real Redis integration suite in `tests/upstash.test.ts`. Those tests use a
unique namespace and clean up their keys. Without those variables, the
integration tests are skipped.
