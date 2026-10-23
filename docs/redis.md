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

## Scanning and cursors

Each claim evaluates at most `min(scanLimit, 100)` candidates, oldest first. The
requester and `excludeIds` are skipped without using up that budget, so a long
exclusion list cannot hide the candidates behind it.

When nothing in the window matches and more entries remain, the `none` result
carries a `cursor`. Pass it back to continue after the last entry examined:

```ts
let cursor: string | undefined;
for (let page = 0; page < 20; page += 1) {
  const result = await matcher.findMatch({ requesterId, poolKey, filter, cursor });
  if (result.status !== "none" || !result.cursor) break;
  cursor = result.cursor;
}
```

Every call is its own bounded, atomic script, so Redis is never blocked for a
whole-pool scan. Entries that join after the cursor's position are still
reached by later pages. The cursor is opaque; treat it as a string. Cap the
number of pages per request, and keep coarse hard constraints in the pool key
(region, mode, media type) so most pages contain viable candidates.

With `strategy: { type: "random" }`, the scan starts at an offset chosen by the
matcher's `random` function and wraps around inside the current window. `fifo`
starts at the oldest entry.

`matched` and `none` results include two counters for capacity monitoring:

- `scanned`: candidates examined, excluding the requester and `excludeIds`
- `stale`: waiting entries removed because their participant record expired

## Exclusions and presence

Claims skip candidates that are excluded in either direction or have gone
quiet, so a claim is not wasted on a pair the application would reject:

- `participant.exclusions` maps other participant IDs to the time (epoch ms)
  the exclusion ends. A candidate is skipped while the requester excludes it
  or it excludes the requester. Use this for "recently left" or "skipped".
- `maxHeartbeatAgeMs` on `findMatch()` skips candidates whose
  `lastHeartbeatAt` is older than that. Stale candidates stay in the index, so
  a later heartbeat makes them eligible again.

## Syncing participants

When the application owns the authoritative participant record, mirror it into
Redis with `syncParticipant()`. One script writes the record and its
waiting-index entries:

```ts
const result = await matcher.syncParticipant({
  id: "alice",
  preferences: { interests: ["music"] },
  state: "waiting",
  pools: ["chat"],
  joinedAt: connectedAt,
  lastHeartbeatAt: lastSeen,
  exclusions: { bob: leftAt + 5 * 60_000 },
  revision: appRecordRevision,
});
```

- The record is created when it is missing. The store assigns the next
  `version`.
- A participant in `claimed` is never overwritten (`status: "claimed"`). The
  claim's commit, rollback, or expiry decides what happens next.
- `expectedVersion` makes the write conditional on the stored version (`0`
  means "must not exist"). A mismatch returns `conflict`.
- `revision` is the application's own monotonic counter. A write with a
  revision at or below the stored one returns `stale`, so syncs that arrive out
  of order cannot roll a record back. Keep the revision monotonic for as long
  as the Redis record lives.
- Non-waiting states remove the participant from every pool it was indexed in.
  A pool change moves it.

## Claim expiry index and recovery

Reserved claims are kept in a sorted set scored by `expiresAt`. Commit, rollback,
and recovery remove claims from it. `recover()` and
`recoverExpiredClaims(now, hasDurableMatch, { limit })` read only the expired
slice, oldest first, and handle at most `limit` claims per call (default 100).
Run recovery on a schedule and call it again when a pass returns a full batch.

Earlier releases tracked claims in an unsorted set (`<namespace>:{<namespace>}:claims`).
Each recovery pass moves up to `limit` entries from that set into the sorted
index, so no manual migration is needed.

Committed claims are retained for `claimRetentionSeconds`, which is also how
long an idempotent retry can replay them. See [Idempotency](idempotency.md).

## Namespaces and deployment

Use a unique namespace per application environment and tenant boundary. Keep
related keys in the same Redis Cluster hash slot when using Redis Cluster.
Partition large populations into coarse pools such as region, mode, or tenant.

The adapter applies TTLs to participant and claim records. Run
`matcher.recover()` from a scheduled worker to recover expired claims and
reconcile them with the durable persistence store. A claim backed by a durable
match is completed even if one participant has changed since; only the
participants still held by the claim move to `matched`.

The Redis adapter is horizontally safe for its supported claim contract, but
should still be load-tested with your pool sizes, filter complexity, and Redis
deployment.
