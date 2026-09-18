# MatchForge architecture

MatchForge is a framework-agnostic TypeScript matchmaking core. Version one matches
pairs and deliberately does not own transport, authentication, chat delivery, or
application database schemas.

## Module layout

- `src/types.ts` — public contracts and domain types
- `src/schemas.ts` — Zod runtime validation
- `src/matcher.ts` — public orchestration facade
- `src/store.ts` — in-memory coordination implementation
- `src/redis.ts` — Redis/Lua coordination implementation
- `src/persistence.ts` — in-memory durable-match implementation
- `src/selection.ts` — candidate selection and relaxation
- `src/utils.ts` — defensive cloning helpers

## Correctness boundary

`CoordinationStore.atomicClaim()` is the central boundary. It validates that the
requester and candidate are still waiting, evaluates the configured rules, removes
both entries from the waiting index, changes both states to `claimed`, and writes a
short-lived claim as one operation.

The in-memory adapter serializes this operation with a promise queue. A future
Redis adapter can implement the same contract with a Lua script without changing
the `Matcher` API.

## Claim lifecycle

1. `findMatch()` atomically reserves two participants.
2. If configured, the matcher writes an idempotent `MatchRecord`.
3. The claim is committed and participants become `matched`.
4. Persistence failure rolls the claim back and requeues both participants.
5. Uncommitted claims can be recovered after their TTL. Recovery checks participant
   versions before requeueing, so newer state is not overwritten.

## Current scope

The core supports FIFO, random, best-score, weighted-random, and custom selection;
compatibility predicates; score functions; relaxation policies; skip lists;
heartbeats; idempotency keys; and metrics-ready interfaces.

The in-memory adapter is for tests, local development, and single-process use. It
is not horizontally safe. Distributed adapters must implement the same interfaces
before production multi-instance deployment.
