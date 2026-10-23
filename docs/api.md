# API quick reference

The root package re-exports the public API from focused modules. Consumers do
not need to import internal files.

```ts
import { InMemoryCoordinationStore, Matcher } from "matchforge";

const matcher = new Matcher({
  store: new InMemoryCoordinationStore(),
});

await matcher.register({
  id: "alice",
  preferences: { language: "en" },
});
await matcher.enqueue("alice", ["chat:en"]);
```

Public participant, preference, pool, and match-request inputs are validated with
Zod. Preferences must be JSON-serializable and no larger than 8 KiB.

Call `findMatch()` with a compatibility predicate for hard constraints or a score
function for ranking:

```ts
const result = await matcher.findMatch({
  requesterId: "alice",
  poolKey: "chat:en",
  strategy: { type: "best-score" },
  compatibility: (a, b) => a.preferences.language === b.preferences.language,
  score: (a, b) => Number(b.preferences.interestsInCommon ?? 0),
});
```

Distributed adapters require a serializable filter. Filters are domain-neutral
and use JSON paths:

```ts
const filter = {
  op: "all",
  filters: [
    {
      op: "overlaps",
      left: { source: "requester", path: ["interests"] },
      right: { source: "candidate", path: ["interests"] },
    },
  ],
} as const;
```

For symmetric compatibility rules, build the AST without embedding domain
knowledge in the adapter:

```ts
import { buildMutualCompatibilityFilter } from "matchforge";

const filter = buildMutualCompatibilityFilter({
  rules: [
    {
      mode: "equals",
      requesterPath: ["category"],
      candidatePath: ["category"],
      wildcardValues: ["any"],
    },
    {
      mode: "overlaps",
      requesterPath: ["tags"],
      candidatePath: ["tags"],
      emptyMatchesAll: true,
    },
  ],
});
```

The Redis adapter supports `all`, `any`, `not`, `overlaps`, `equals`, `in`,
`contains`, and `is-empty` with FIFO or random selection. It rejects arbitrary runtime
callbacks because they cannot execute inside an atomic Redis Lua script.

`findMatch()` also accepts:

- `cursor`: continue a search from a previous `none` result's `cursor`.
- `maxHeartbeatAgeMs`: skip candidates whose last heartbeat is older than this.
- `idempotencyKey`: make retries safe. See [Idempotency](idempotency.md).

Participants can carry `exclusions`, a map of other participant IDs to the time
(epoch ms) each exclusion ends. Claims honour it in both directions.

`syncParticipant()` writes a participant's full coordination state and its
waiting-index entries in one atomic step. It never overwrites a participant
held by a claim, and it supports `expectedVersion` and caller `revision`
checks. See [Redis usage](redis.md#syncing-participants).

`recover({ limit })` handles at most `limit` expired claims per call (default 100).

Use a `PersistenceStore` in production. The coordination store owns ephemeral
waiting and claims; the persistence store owns durable matches.
