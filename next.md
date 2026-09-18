# MatchForge remaining work

This document records work that remains after the initial core, tests, refactor,
and documentation pass. It is deliberately implementation-oriented. “Done” means
implemented, tested, documented, and suitable for the stated deployment target,
not merely represented by an interface.

## Current baseline

Implemented in the current repository:

- Pair-only matchmaking types and state transitions.
- `Matcher` orchestration facade.
- In-memory `CoordinationStore` with serialized atomic claims.
- In-memory `PersistenceStore`.
- FIFO, random, best-score, weighted-random, and custom selection.
- Compatibility predicates, score functions, and relaxation policies.
- Claim commit, rollback, TTL expiry, and version-checked requeue.
- Basic heartbeat timestamps and `isAlive()`.
- Zod validation for participant, preference, pool, and match-request inputs.
- Concurrency, idempotency, scoring, exclusion, expiry, and validation tests.
- Focused source modules and ADR/topic documentation.

The current implementation is a single-process reference implementation. It is
not yet safe to use as the coordination layer for horizontally scaled workers.

## Priority 0: correctness gaps in the current core

These should be addressed before calling the in-memory implementation a stable
v1 core.

### 1. Make persistence recovery explicit

Current behavior:

- A claim is reserved.
- `saveMatch()` is called.
- The claim is committed afterward.
- If either persistence or commit fails, the matcher attempts rollback.

Gap:

- If `saveMatch()` succeeds and the process fails before `commitClaim()`, the
  durable match can exist while the claim remains reserved.
- `recover()` only expires and requeues reserved claims. It does not check the
  durable store for an existing match.
- Rollback after a partial persistence failure does not delete or mark a
  previously written durable record.

Required work:

- Add a reconciliation operation that checks each expired/reserved claim against
  `PersistenceStore.getMatch(matchId)`.
- If the durable match exists, complete the claim instead of requeueing.
- If it does not exist, expire and requeue participants using version checks.
- Define behavior for a durable record with a conflicting participant set.
- Add an optional `deleteMatch()` or `markMatchFailed()` persistence capability
  for applications that need cleanup after partial writes.
- Add fault-injection tests for failures before save, after save, before commit,
  during commit, and during rollback.

Acceptance criteria:

- A crash at every boundary has one deterministic recovery outcome.
- Recovery is idempotent and safe to run repeatedly.
- A participant cannot be requeued when a durable active match exists.

### 2. Fix multi-pool lifecycle semantics

Current behavior:

- A participant may be enqueued into multiple pools.
- A claim removes the participant only from the selected pool.
- Other index entries remain, but state checks make them stale.
- Rollback and expiry requeue only the claim’s pool.

Required work:

- Decide whether multi-pool membership means “eligible in any pool” or should be
  represented by one active lane at a time.
- Remove all waiting-index entries when a participant is claimed, matched, left,
  or cancelled.
- Reinsert all still-valid pools on rollback or expiry, or document a single-pool
  invariant and enforce it with Zod.
- Add tests for claiming from one pool while another pool remains indexed.

Acceptance criteria:

- Queue depth does not retain stale entries after normal state transitions.
- Rollback restores exactly the participant’s intended queue membership.

### 3. Define participant lifecycle and duplicate registration rules

Current behavior:

- `register()` overwrites an existing participant with the same ID.
- `dequeue()` marks a participant `left` but does not validate the identifier
  before querying the store.
- `isAlive()` reports a boolean but does not transition a participant to `away`.
- `cancelled`, `away`, and `expired` states are declared but not fully managed.

Required work:

- Make registration either reject duplicates or provide an explicit upsert
  command with version control.
- Add `cancel()` and a documented state transition table.
- Add configurable heartbeat expiry/reconciliation that marks stale participants
  `away` and removes them from indexes.
- Decide whether `away` participants can be automatically requeued after a
  heartbeat.
- Validate IDs consistently across all public commands.

Acceptance criteria:

- Every declared state has documented entry and exit transitions.
- Duplicate and stale commands are deterministic and covered by tests.

## Priority 1: distributed production adapter

### 4. Implement `@matchforge/adapter-redis`

The original plan identifies Redis/Lua as the first production adapter. It is not
implemented yet.

Required components:

- Redis key namespace with tenant and pool isolation.
- Participant records with versioned serialization.
- Waiting indexes, preferably sorted by join time.
- Claim records with TTL.
- Idempotency records with TTL.
- Versioned Lua claim script.
- Atomic Lua path for requester validation, candidate scan, filtering supported by
  serialized data, selection, queue removal, participant state changes, and claim
  creation.
- Claim commit, rollback, expiry, and recovery operations.
- Native Redis client adapter.
- REST/Upstash-compatible adapter or a shared command abstraction.

Important design constraint:

- Arbitrary TypeScript compatibility and score functions cannot execute inside
  Redis Lua. The distributed API must either restrict script-side predicates to
  serializable filters, accept precomputed score hints, or use a clearly
  documented two-phase reservation flow.
- Do not claim that a distributed adapter supports arbitrary async rule functions
  atomically unless the design actually proves that property.

Acceptance criteria:

- Two concurrent workers cannot claim the same participant.
- Redis Cluster key placement is documented and tested.
- Script versions and key schemas are versioned.
- Contract tests pass for both memory and Redis adapters.
- TTL cleanup and recovery work after simulated worker crashes.

### 5. Move idempotency into the coordination store

Current behavior:

- Idempotency is held in a process-local `Map` inside `Matcher`.
- It is lost on restart and is not shared across workers.
- Only successful matched claims are cached.

Required work:

- Add idempotency operations to `CoordinationStore`, or make them part of
  `atomicClaim()`.
- Atomically reserve and retrieve idempotency keys.
- Store outcome, claim ID, match ID, and expiry.
- Define behavior for repeated `none`, `stale`, and `error` outcomes.
- Reject reuse of one key with materially different request parameters.

Acceptance criteria:

- A retry routed to another process returns the original outcome.
- A retry after process restart remains safe within the configured TTL.

## Priority 1: durable persistence contract

### 6. Publish a reference database persistence example

The plan calls for a documented Postgres example, not necessarily a bundled
adapter.

Required work:

- Document an idempotent `saveMatch()` transaction keyed by `matchId`.
- Show unique constraints preventing duplicate participant matches.
- Show how active matches are queried during claim reconciliation.
- Document transaction isolation and what the coordination store does not solve.
- Keep the example database-specific and outside the zero-dependency core.

### 7. Expand match records without coupling the core to applications

Current `MatchRecord` contains only IDs, pool, timestamps, and status.

Required work:

- Decide whether to support a typed `metadata` field or a generic application
  payload reference.
- Define size and privacy rules if metadata is added.
- Keep chat rooms, game sessions, transport details, and authentication outside
  the core package.

## Priority 1: observability and operational behavior

### 8. Wire metrics and logging into the matcher

`MetricsSink` and `Logger` types exist, but `Matcher` currently does not emit
metrics or structured logs.

Required work:

- Count matched, none, stale, error, rollback, expiry, and duplicate outcomes.
- Record claim latency and wait time.
- Record candidate scan counts and queue depth.
- Emit adapter and persistence failures.
- Never log raw preferences.
- Make instrumentation failure non-fatal to matching.

Acceptance criteria:

- A no-op/default configuration has no observable overhead beyond normal calls.
- A supplied sink receives stable metric names and bounded tag values.
- Operational docs include dashboards and alert thresholds.

### 9. Add a sweeper interface

Current recovery is opportunistic through `Matcher.recover()`. There is no
background scheduler or adapter-level scan contract.

Required work:

- Add a documented sweeper recipe rather than embedding a permanent worker in
  the core.
- Define claim pagination and “near expiry” scans for Redis.
- Make the sweeper safe to run concurrently.
- Add backoff and maximum work per pass.
- Document recommended cadence relative to claim TTL.

## Priority 2: test depth

### 10. Add shared adapter contract tests

The current tests cover the matcher with the in-memory store only.

Required test groups:

- Participant CRUD and version conflicts.
- Queue insertion/removal and stale index entries.
- Atomic concurrent claims.
- Commit, rollback, expiry, and recovery.
- Idempotency across repeated and concurrent requests.
- Multi-pool behavior.
- Persistence failure and reconciliation.

The same suite should run against memory and Redis implementations.

### 11. Add property and model-based tests

Generate random command sequences and assert invariants:

- A participant is in at most one live claim.
- A participant is not both waiting and claimed.
- A committed match has the claimed participant versions.
- Rollback and expiry do not overwrite newer versions.
- Queue indexes do not create claimable entries for non-waiting participants.

### 12. Add load and chaos tests

Target scenarios from the original plan:

- 10,000 or more waiting participants.
- Sustained claim traffic.
- Hot single-pool contention.
- Candidate changes during scans.
- Process termination between claim, persistence, and commit.
- Redis latency and transient errors.

Record p50/p95/p99 latency, scan counts, match rate, expiry rate, and memory
growth.

## Priority 2: packaging and release readiness

### 13. Turn the repository into a publishable package

Current state:

- Package name is `matchforge`.
- The package is private.
- `module` points to TypeScript source.
- There is no build output, declaration output, exports map, or publish workflow.

Required work:

- Confirm the final npm name and scope.
- Decide ESM-only versus dual ESM/CommonJS support.
- Build JavaScript and declaration output.
- Add an `exports` map with stable public entry points.
- Add `files` allowlisting.
- Remove development-only files from published output.
- Add Node/Bun/edge runtime compatibility guidance.
- Pin supported TypeScript and runtime versions.
- Add package size and install smoke tests.
- Add provenance-enabled npm publishing in CI.

### 14. Add CI quality gates

Required checks:

- Bun tests.
- Typecheck.
- Formatting/linting.
- `git diff --check`.
- Package build.
- Consumer import smoke test.
- Redis integration tests when the adapter exists.
- Documentation link checks.

## Priority 2: examples and user experience

### 15. Add runnable examples

Create small examples for:

- Anonymous language-matched chat.
- Support queue with priority scoring.
- Ranked game matching with a score window.
- Relaxation after a wait threshold.
- Durable persistence and recovery.

Examples must make it obvious that the in-memory adapter is not horizontally
safe.

### 16. Improve API ergonomics and generated reference docs

Required work:

- Add explicit return types to public methods such as `findMatch()`.
- Add JSDoc to exported interfaces, schemas, and lifecycle methods.
- Decide whether schemas are part of the supported public API.
- Generate API documentation from TypeScript declarations.
- Add migration notes for claim/schema changes.

## Deferred by explicit product decision

These are not accidental omissions; they were excluded from the first release:

- Group and multi-party matching.
- Capacity-based assignment.
- Declarative rule engines.
- Full Postgres adapter.
- Multi-region active-active coordination.
- Built-in chat, WebRTC, WebSocket, SSE, authentication, or UI.
- Hosted matchmaking service.
- CRDT-based replicated queues.

When group matching is revisited, preserve the current atomic-claim abstraction
but introduce explicit `minSize`, `maxSize`, group selection, partial-claim
rollback, and group-level version/recovery tests. Do not silently widen the
pair-only API.

## Suggested implementation order

1. Fix multi-pool and lifecycle semantics.
2. Add durable claim reconciliation and fault-injection tests.
3. Move idempotency into the coordination-store contract.
4. Implement Redis/Lua with adapter contract tests.
5. Wire metrics, logging, and sweeper guidance.
6. Add Postgres persistence guidance and runnable examples.
7. Add package build, CI, and release automation.
8. Reassess group matching only after pair correctness is proven under load.

---

# Scale research and non-blocking matchmaking plan

## Executive conclusion

The current implementation provides useful correctness primitives, but it is
not yet a high-throughput matchmaking service architecture.

It can store 10,000 waiting participants in one process, but it currently uses a
single serialized executor for all coordination operations. `atomicClaim()` also
awaits compatibility and scoring functions while holding that serialized slot.
A slow rule function can therefore delay every other claim in that store.

The target architecture for 10,000+ participants is:

1. Accept a matchmaking ticket quickly.
2. Return a ticket ID without waiting for a match.
3. Partition tickets into compatible queue shards.
4. Run dedicated matcher workers asynchronously.
5. Process bounded batches instead of scanning the entire queue.
6. Keep the atomic reservation section short.
7. Persist the durable match outside the coordination lock.
8. Notify or expose the result through polling, SSE, or WebSocket.

The important distinction is:

- **Queue size**: how many people are waiting.
- **Request concurrency**: how many API calls arrive at once.
- **Matchmaking work**: how many candidates and rules are evaluated per cycle.

10,000 waiting users is manageable. 10,000 users in one hot pool, all invoking
expensive asynchronous rules through one serialized executor, is not.

## What the current code already contributes

The current implementation has several correct building blocks:

- Bounded candidate scans through `scanLimit`.
- Atomic claim semantics through `CoordinationStore.atomicClaim()`.
- Participant version checks.
- Claim expiry, rollback, and requeue.
- Multiple candidate-selection strategies.
- Separation of coordination state and durable match state.
- Runtime validation and preference size limits.

These should be preserved while changing the execution model around them.

## Current blocking points

### A. The in-memory store is single-filed

`InMemoryCoordinationStore` uses a serial promise executor. This prevents two
claims from racing, but every operation shares one queue:

- participant reads and writes
- enqueue and dequeue
- queue-index updates
- candidate scans
- rule evaluation
- claim creation
- claim recovery

The design is safe for a single process, but throughput is bounded by the
slowest operation currently running in that executor.

### B. Async rules run inside the critical section

The store currently awaits:

```ts
await input.compatibility(...)
await input.score(...)
```

while the serial executor is holding the operation slot.

Rules must not perform:

- database calls
- HTTP requests
- Redis calls
- file access
- remote feature lookup
- unbounded loops
- expensive model inference

The core should either require fast pure rules or expose a two-phase worker flow
where expensive evaluation happens before the final atomic reservation.

### C. Redis Lua would also block during execution

Moving the current algorithm directly into Redis Lua would improve distributed
atomicity, but it would not make unbounded work non-blocking. Redis guarantees
atomic script execution by blocking other server activity for the duration of
the script.

Therefore, a Redis script must:

- inspect a bounded number of candidates
- avoid full-queue scans
- avoid large JSON payloads
- avoid network calls
- avoid expensive scoring
- finish within a strict time budget

Source: [Redis Lua scripting](https://redis.io/docs/latest/develop/programmability/eval-intro)

### D. Process-local idempotency does not survive scaling

The current matcher keeps idempotency in a process-local `Map`. This fails when:

- a retry reaches another service instance
- the process restarts
- a deployment replaces the instance
- multiple regions receive requests

Idempotency records must move into the coordination store and be part of the
distributed claim protocol.

## Research findings from comparable systems

### Open Match

Open Match uses a ticket-oriented, service-separated design. Its architecture
includes frontend services, ticket storage/querying, match functions,
evaluators, synchronizers, directors, and backfill flows.

The useful pattern is that the player request creates a ticket instead of
holding an API request open while the system searches synchronously. Workers
fetch bounded ticket pools, evaluate matches, synchronize state, and release or
assign tickets.

Relevant sources:

- [Open Match overview](https://open-match.dev/site/docs/overview/)
- [Open Match matchmaking guide](https://open-match.dev/site/docs/guides/matchmaker)
- [Open Match evaluator and synchronizer](https://open-match.dev/site/docs/guides/evaluator)
- [Open Match production best practices](https://open-match.dev/site/docs/guides/production)
- [Open Match installation and horizontal scaling](https://open-match.dev/site/docs/installation/yaml/)
- [Open Match API reference](https://open-match.dev/site/docs/reference/api/)

Implementation lesson:

MatchForge should separate:

- ticket admission
- candidate retrieval
- rule evaluation
- reservation
- durable assignment
- notification

### Amazon GameLift FlexMatch

FlexMatch uses ticket pools, rules, batch formation, ticket age, rule
expansion, and backfill. AWS documents that FlexMatch forms additional batches
as pools grow and sorts tickets by age within batches.

This avoids comparing every ticket against every other ticket. A practical
algorithm is:

1. Select the oldest waiting ticket.
2. Build a bounded batch around it.
3. Apply hard constraints.
4. Score eligible candidates.
5. Commit a match if possible.
6. Expand the search window or relax rules after a wait threshold.

Relevant sources:

- [FlexMatch match algorithm](https://docs.aws.amazon.com/gameliftservers/latest/flexmatchguide/match-rules-algorithm.html)
- [FlexMatch rule-set properties](https://docs.aws.amazon.com/gameliftservers/latest/flexmatchguide/match-ruleset-property-definitions.html)
- [GameLift matchmaking tickets](https://docs.aws.amazon.com/gameliftservers/latest/apireference/API_MatchmakingTicket.html)

Implementation lesson:

Add oldest-ticket anchoring, bounded batches, and explicit relaxation stages.
Do not make a global “best match across the entire queue” the default.

### PlayFab Matchmaking

PlayFab models matchmaking around queues, tickets, and queue-specific rules.
Different populations can be isolated into separate queues rather than sharing
one global scan.

Relevant sources:

- [PlayFab matchmaking](https://learn.microsoft.com/en-us/xbox/playfab/multiplayer/matchmaking)
- [Configuring PlayFab matchmaking queues](https://learn.microsoft.com/en-us/gaming/playfab/multiplayer/matchmaking/config-queues)
- [PlayFab matchmaking scenarios](https://learn.microsoft.com/en-us/gaming/playfab/multiplayer/matchmaking/configuration-examples)

Implementation lesson:

Pool configuration should be explicit and deployable:

- queue name
- tenant
- region
- game mode
- ticket age policy
- candidate batch size
- relaxation policy
- match size
- backfill policy

### Nakama

Nakama exposes a ticket-oriented matchmaker and query syntax for selecting
relevant players or matches.

Relevant sources:

- [Nakama matchmaker](https://heroiclabs.com/docs/nakama/concepts/multiplayer/matchmaker/)
- [Nakama query syntax](https://heroiclabs.com/docs/nakama/concepts/multiplayer/query-syntax/)

Implementation lesson:

The client should submit a ticket and observe ticket status. It should not need
to issue a new expensive candidate search on every request or poll cycle.

### Redis sorted sets and Lua

Redis sorted sets are a natural queue primitive because entries can be ordered
by join time or a numeric hint. Redis documents sorted set insertion as
`O(log N)`.

Relevant sources:

- [Redis sorted sets](https://redis.io/docs/latest/develop/data-types/sorted-sets)
- [Redis Lua scripting](https://redis.io/docs/latest/develop/programmability/eval-intro)
- [Redis Cluster scaling](https://redis.io/docs/latest/operate/oss_and_stack/management/scaling)
- [Redis Cluster specification](https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec)

Implementation lesson:

Redis is suitable for the coordination layer, but one giant hot key is not a
complete scaling strategy. Use queue shards and keep related atomic keys in the
same Redis Cluster hash slot.

### Partitioned queue systems

Partitioned systems such as Kafka demonstrate a broader pattern: work is
distributed by partition key, and one consumer owns ordered work within a
partition while many consumers process different partitions.

Relevant source:

- [Kafka queue proposal and partition consumers](https://www.morling.dev/blog/kip-932-queues-for-kafka/)

Implementation lesson:

MatchForge should use the same conceptual model for worker ownership:

- one logical worker owner per queue shard
- many queue shards per service
- horizontal scaling by assigning shards to workers
- no global mutex around all tenants and pools

Kafka itself is not necessarily the right final coordination store for low-latency
claims, but its partitioning model is useful.

## Target non-blocking architecture

### Layer 1: ticket API

The API should return quickly:

```text
POST /matchmaking/tickets
  validate input
  create ticket
  enqueue ticket
  return ticketId
```

It should not wait for a match.

The client can then use one of:

- `GET /matchmaking/tickets/:id`
- long polling
- Server-Sent Events
- WebSocket notification
- application event subscription

### Layer 2: queue partitioning

Do not put every participant into one global queue.

Use coarse compatibility dimensions first:

- tenant
- region
- game mode
- platform
- language
- latency class
- skill band

Then optionally add an internal shard:

```text
matchmaking:{tenant}:{region}:{mode}:{shard}
```

For example:

```text
matchmaking:tenant-a:eu-west:ranked:03
matchmaking:tenant-a:eu-west:ranked:04
matchmaking:tenant-a:eu-west:ranked:05
```

Pure participant hashing can prevent compatible participants from meeting.
Prefer coarse compatibility partitioning followed by a hash shard. Older
tickets should be allowed to search adjacent shards or a wider pool after a
defined wait threshold.

### Layer 3: matcher workers

Workers should process queues independently:

```text
queue shard 1 -> worker 1
queue shard 2 -> worker 2
queue shard 3 -> worker 3
```

Each worker should:

1. select an oldest ticket
2. retrieve a bounded candidate batch
3. run fast pure rules
4. select a candidate or group
5. atomically reserve the participants
6. persist the durable match
7. commit or roll back
8. publish a result event

The API service remains stateless and horizontally scalable.

### Layer 4: bounded work budgets

Every worker cycle should have explicit limits:

```text
maximum tickets per cycle: 100
maximum candidates per ticket: 50–200
maximum cycle duration: 10–50 ms
maximum rule evaluation time: configured and measured
```

If a cycle exceeds its budget:

- stop processing
- retain or release unprocessed tickets
- record the partial work
- continue in the next cycle

One hot queue must not starve other shards.

### Layer 5: final atomic reservation

The atomic section should only:

- verify ticket state
- verify participant versions
- verify selected candidates are still available
- remove queue entries
- create a short-lived reservation
- set a TTL

It should not:

- call a database
- call another service
- send notifications
- allocate a game server
- run expensive scoring
- write large preference blobs

## Two-phase advanced matching

Arbitrary TypeScript functions cannot execute inside Redis Lua. There are two
viable designs.

### Design A: serializable fast filters

Only allow rules that can be represented as data:

```ts
{
  language: { equals: "en" },
  skill: { between: [1200, 1400] }
}
```

Redis can apply these filters or use precomputed indexes.

Advantages:

- strongest atomicity
- short scripts
- predictable latency
- easy horizontal scaling

Disadvantages:

- less expressive than arbitrary functions
- requires a rule schema and versioning

### Design B: worker evaluation then reservation

Workers retrieve bounded candidates and run TypeScript rules outside the Redis
critical section. They then attempt a final atomic reservation with participant
versions.

Advantages:

- supports rich application rules
- rules can use local in-memory data
- easier migration from the current API

Disadvantages:

- candidates can disappear between evaluation and reservation
- retries are required
- reservation conflict rates must be monitored

Recommended path:

Use Design B first for flexibility, then add Design A for high-volume simple
queues.

## Proposed MatchForge API evolution

Keep the current low-level API for compatibility, but add ticket and worker
abstractions.

### Ticket API

```ts
interface MatchmakingTicket {
  id: string;
  participantId: string;
  poolKey: string;
  shardKey: string;
  state: "queued" | "reserved" | "matched" | "cancelled" | "expired";
  createdAt: number;
  updatedAt: number;
  version: number;
  expiresAt?: number;
}

interface TicketMatcher {
  createTicket(input: CreateTicketInput): Promise<MatchmakingTicket>;
  getTicket(ticketId: string): Promise<MatchmakingTicket | null>;
  cancelTicket(ticketId: string): Promise<void>;
}
```

### Worker API

```ts
interface MatchWorkerOptions {
  shardKey: string;
  maxCandidates: number;
  maxTicketsPerCycle: number;
  maxCycleMs: number;
}

interface MatchWorker {
  runOnce(): Promise<WorkerCycleResult>;
}
```

`runOnce()` is preferable to embedding an infinite loop in the package. Users
can run it from a scheduler, worker process, Kubernetes deployment, or actor
runtime.

## Load and capacity plan

The repository should not claim 10,000+ support until these measurements exist.

### Initial benchmark scenarios

1. 10,000 waiting participants across 100 shards.
2. 10,000 waiting participants in one hot shard.
3. 1,000 concurrent ticket submissions.
4. 1,000 concurrent match attempts.
5. 10 ms pure compatibility function.
6. 100 ms compatibility function.
7. 1% reservation conflicts.
8. Persistence latency at p50, p95, and p99.
9. Claim expiry during persistence.
10. Worker restart during reservation.

### Required measurements

- ticket admission latency
- match attempt latency
- p50/p95/p99 claim latency
- candidates examined per match
- match success rate
- stale candidate rate
- reservation conflict rate
- rollback rate
- expiry rate
- queue age percentiles
- worker CPU and memory
- Redis command latency
- Redis script duration
- persistence latency
- notification latency

### Suggested initial success criteria

These numbers should be tuned to the deployment, but a first benchmark should
at least define:

- ticket admission p99 target
- matching-cycle p99 target
- maximum Redis script duration
- maximum queue age under steady load
- maximum acceptable stale reservation rate
- maximum worker CPU utilization

Do not use “10,000 users supported” as the only capacity metric.

## Deployment options

### Option A: integrate Open Match

Best when matchmaking becomes a major game platform capability.

Use Open Match for ticket lifecycle, evaluation orchestration, synchronizers,
and Kubernetes deployment. Use MatchForge for domain-specific preference
validation, application metadata, and persistence integration.

### Option B: use a managed provider

Best when infrastructure ownership is not a product differentiator.

Candidates include:

- Amazon GameLift FlexMatch
- PlayFab Matchmaking
- Unity Matchmaker
- Nakama

MatchForge can remain a local rules and integration layer, or it can be omitted
if the managed provider fully covers the requirements.

### Option C: implement Redis shards and workers

Best when MatchForge must remain an independent package:

```text
stateless API service
        |
        v
distributed ticket store
        |
        v
sharded matcher workers
        |
        v
durable match persistence
        |
        v
notification/event service
```

This is the most flexible option and the largest engineering commitment.

## Scale-focused implementation order

The following order is intended to allow one independently testable task at a
time.

### Scale task 1: remove rule I/O from the store critical section

- Document rules as pure and bounded.
- Add a timeout or work budget at the worker layer.
- Add a test proving a slow rule cannot block unrelated shards.
- Do not use network-backed rule functions in `atomicClaim()`.

### Scale task 2: define ticket lifecycle

- Add ticket types and schemas.
- Add ticket create, read, cancel, expire, and status transitions.
- Keep ticket admission independent from matching.
- Add idempotent ticket creation.

### Scale task 3: define queue-shard configuration

- Add tenant and shard identity to pool configuration.
- Define coarse partition keys.
- Define shard count and rebalancing behavior.
- Add queue-age and cross-shard relaxation policies.

### Scale task 4: add a bounded worker cycle

- Select the oldest ticket.
- Fetch a bounded candidate batch.
- Apply pure rules outside the final claim.
- Reserve with version checks.
- Return a cycle result with metrics.

### Scale task 5: move idempotency into coordination

- Persist ticket and match request keys.
- Make retries safe across worker instances.
- Define key reuse conflicts.
- Test restart and cross-worker retries.

### Scale task 6: implement Redis coordination

- Add sorted-set queue indexes.
- Add participant and ticket records.
- Add reservation TTLs.
- Add versioned Lua scripts.
- Keep scripts bounded and short.
- Add Redis Cluster key-tag tests.

### Scale task 7: add persistence reconciliation

- Reconcile claims after worker crashes.
- Check durable matches before requeue.
- Make recovery idempotent.
- Add partial-write fault tests.

### Scale task 8: add notifications

- Publish match-created events.
- Add polling and optional SSE/WebSocket integration examples.
- Make notification delivery retry-safe.
- Keep transport outside the core package.

### Scale task 9: benchmark and chaos test

- Run the benchmark matrix above.
- Run 10,000+ participant tests.
- Kill workers between every claim lifecycle step.
- Record p99 and failure rates.
- Tune shard and batch sizes from measurements.

### Scale task 10: package and operate

- Add build output and declaration files.
- Add CI and integration tests.
- Add dashboards and alerts.
- Publish a guarantees matrix with measured limits.

## Final scale assessment

The current code is a correct starting point for a single-process adapter, not
the final architecture for a large production matchmaking service.

The research consistently points toward the same design:

> Accept matchmaking requests quickly, store tickets, process bounded batches
> asynchronously, partition hot queues, and use atomic reservation only for the
> small final claim step.

The first production milestone should therefore be a ticket-based worker model,
not simply increasing `scanLimit` or replacing the in-memory map with Redis.
