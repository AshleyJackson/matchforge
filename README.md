# MatchForge

Framework-agnostic TypeScript matchmaking primitives for correct pair matching.

## Status

This repository contains the v1 core, a first-class in-memory coordination
adapter, and a Redis/Lua coordination adapter. The Redis adapter is intended
for coarse, pre-partitioned pools and currently supports FIFO claims only.
Arbitrary TypeScript compatibility and scoring functions remain in-process
features.

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

## Development

```bash
bun test
bunx tsc --noEmit
```

See [`docs/architecture.md`](docs/architecture.md) and
[`docs/api.md`](docs/api.md) for the claim protocol and API details.
The complete documentation index is [`docs/README.md`](docs/README.md), including
the architecture decision records in [`docs/adr/`](docs/adr/).
