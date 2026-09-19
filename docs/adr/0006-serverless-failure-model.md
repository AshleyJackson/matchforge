# ADR-0006: Provider-neutral serverless failure model

- Status: Accepted
- Date: 2026-09-19

## Context

MatchForge is intended to run behind serverless request handlers. Cloudflare
Workers use evictable isolates, Vercel Fluid Compute can run concurrent
invocations in a reusable function instance, and AWS Lambda can freeze, reset,
reuse, and later terminate execution environments.

These platforms do not provide a shared lifecycle contract that applications
can use for correctness. A request may stop after a Redis mutation or database
write, and in-memory cleanup may not execute.

## Decision

MatchForge models worker termination as an abrupt stop at any asynchronous
boundary. Correctness is based on durable coordination state, idempotent
persistence, version checks, claim TTLs, and explicit reconciliation.

The core does not depend on:

- process shutdown hooks;
- module-level state;
- a successful HTTP response;
- `waitUntil()` completing;
- a provider-specific eviction callback.

Crash simulations use named failure points in the claim lifecycle. The same
failure-point suite runs against the in-memory adapter and a real Redis
adapter.

## Consequences

- Every durable boundary must have a retry or recovery path.
- Tests can be deterministic without pretending to reproduce provider internals.
- Provider-specific smoke tests remain useful for timeout, disconnect, and
  background-task behavior.
- A durable sweeper is part of the deployment, not an optional cleanup detail.
