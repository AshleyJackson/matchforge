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

A distributed adapter must preserve the atomic claim boundary. Redis/Lua is the
planned production adapter. Its script must validate state, select a candidate,
remove both queue entries, update participant state, and write the claim in one
atomic operation.
