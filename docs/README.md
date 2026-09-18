# MatchForge documentation

MatchForge is a framework-agnostic TypeScript core for pair matchmaking.

## Start here

- [API reference](api.md)
- [Concepts](concepts.md)
- [Architecture](architecture.md)
- [Failure recovery](failure-recovery.md)

## Operations and design

- [Adapters](adapters.md)
- [Matching rules](matching-rules.md)
- [Guarantees](guarantees.md)
- [Scaling](scaling.md)
- [Security and privacy](security.md)
- [Observability](observability.md)
- [Versioning](versioning.md)
- [Remaining work](../next.md)

## Architecture decisions

- [ADR index](adr/README.md)

The current repository implements the in-memory coordination and persistence
adapters. Redis/Lua is the next distributed adapter and is not yet shipped.
