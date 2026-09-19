# ADR-0007: Keep authoritative state outside serverless workers

- Status: Accepted
- Date: 2026-09-19

## Context

Serverless instances and isolates may be reused, run concurrently, frozen,
evicted, reset, or replaced. Local memory is useful for caches and connection
reuse, but it cannot coordinate requests across instances.

MatchForge needs to coordinate waiting participants and claims while an
application-specific persistence layer stores durable matches.

## Decision

Authoritative state is divided as follows:

- Redis or another distributed coordination store owns waiting indexes,
  participant versions, claims, idempotency records, and recovery metadata.
- The application persistence store owns durable match records.
- The serverless worker owns only request-local computation, adapters, and
  best-effort caches.

Worker-local shutdown hooks and background tasks may improve latency but must not
be required for correctness.

## Consequences

- Kismet can use MatchForge from Vercel Fluid Compute without relying on a
  particular function instance receiving a retry.
- Redis and persistence operations must be idempotent and observable.
- Tests must create fresh worker instances after injected failures.
- Long-running or retry-heavy work should use a durable queue, workflow, or
  scheduled sweeper rather than an untracked background promise.
