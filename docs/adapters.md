# Adapters

## Coordination store schema

`CoordinationStore` owns ephemeral coordination state:

- participant records
- waiting indexes
- claims
- atomic claim and expiry recovery

`InMemoryCoordinationStore` serializes atomic operations within one process. It
is intended for tests, local development, and single-node deployments only.

## Persistence store schema

`PersistenceStore` owns durable match records. `MemoryPersistenceStore` is
provided for tests. Production applications should implement the parsed Zod
contract with their database and make `saveMatch()` idempotent by `matchId`.

## Distributed adapter contract

A distributed adapter must preserve the atomic claim boundary. The bundled
Redis/Lua adapter requires a Zod-validated serializable filter, evaluates a
bounded recursive filter, selects a FIFO candidate, removes both queue entries,
updates participant state, stores idempotency data, and writes the claim in one
atomic operation.

The current generic filter operators are `all`, `any`, `not`, `overlaps`,
`equals`, `in`, `contains`, and `is-empty`. The exported
`buildMutualCompatibilityFilter()` helper composes these primitives for
symmetric scalar and collection constraints, including configurable wildcard
values and empty-collection behavior.
Operands address requester or candidate preferences through JSON paths or use
literal values. Arbitrary runtime compatibility, scoring, and relaxation
functions cannot run inside Redis Lua and are rejected by the Redis adapter.

The Redis adapter still needs real-server integration tests, cluster-slot
validation, paginated claim recovery, and load testing before production
hardening is complete.

See [Redis usage](redis.md) for an Upstash setup and runnable example.
