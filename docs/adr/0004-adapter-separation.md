# ADR-0004: Separate coordination and persistence adapters

- Status: Accepted
- Date: 2026-09-18

## Decision

Ephemeral waiting and claim state use `CoordinationStore`; durable matches use
`PersistenceStore`. The core does not own a database schema.

## Rationale

Waiting queues need atomic, low-latency coordination, while match records need
application-specific durability and lifecycle fields. Keeping them separate
supports in-memory tests, Redis coordination, and different databases.

## Consequences

Applications must handle the failure window between reservation and durable
persistence. The matcher provides commit, rollback, TTL, and version-aware
recovery primitives for that window.
