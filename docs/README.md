# MatchForge documentation

MatchForge is a framework-agnostic TypeScript core for pair matchmaking.

## Start here

- [API reference](api.md)
- [Concepts](concepts.md)
- [Architecture](architecture.md)
- [Failure recovery](failure-recovery.md)
- [Idempotency](idempotency.md)
- [Serverless execution models](serverless-execution-models.md)
- [Worker-crash simulation](crash-simulation.md)

## Operations and design

- [Adapters](adapters.md)
- [Redis usage](redis.md)
- [Matching rules](matching-rules.md)
- [Guarantees](guarantees.md)
- [Scaling](scaling.md)
- [Security and privacy](security.md)
- [Observability](observability.md)
- [Versioning](versioning.md)
- [Remaining work](../next.md)

## Architecture decisions

- [ADR index](adr/README.md)

The current repository implements in-memory coordination and persistence
adapters plus a Redis/Lua coordination adapter. The Redis adapter supports
coarse, pre-partitioned FIFO or random-selection pools; arbitrary TypeScript rules are not executed
inside Redis.
