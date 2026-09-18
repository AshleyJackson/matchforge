# ADR-0002: Atomic claim as the correctness boundary

- Status: Accepted
- Date: 2026-09-18

## Decision

`CoordinationStore.atomicClaim()` is the sole correctness boundary for removing
participants from a waiting pool and creating a reservation.

## Rationale

Separate read, select, and delete calls allow concurrent workers to claim the
same participant. A serialized in-memory operation and a future Redis Lua script
provide the same contract while allowing different deployment models.

## Consequences

Rules must be bounded and side-effect free. Durable persistence happens after
the reservation and therefore requires idempotency plus rollback or recovery.
