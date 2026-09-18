# Adapters

## CoordinationStore

`CoordinationStore` owns ephemeral coordination state:

- participant records
- waiting indexes
- claims
- atomic claim and expiry recovery

`InMemoryCoordinationStore` serializes atomic operations within one process. It
is intended for tests, local development, and single-node deployments only.

## PersistenceStore

`PersistenceStore` owns durable match records. `MemoryPersistenceStore` is
provided for tests. Production applications should implement this interface with
their database and make `saveMatch()` idempotent by `matchId`.

## Distributed adapter contract

A distributed adapter must preserve the atomic claim boundary. The bundled
Redis/Lua adapter validates state, selects a FIFO candidate from a
pre-partitioned pool, removes both queue entries, updates participant state, and
writes the claim in one atomic operation.

Arbitrary TypeScript compatibility and scoring functions cannot run inside Redis
Lua. Applications must encode hard constraints in pool keys or provide a
purpose-built adapter rule.
