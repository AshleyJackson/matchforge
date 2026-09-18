# ADR-0003: Zod at public input boundaries

- Status: Accepted
- Date: 2026-09-18

## Decision

Zod validates public registration, preference, enqueue, and find-match inputs.
Preferences must be JSON-serializable and at most 8 KiB.

## Rationale

Runtime callers can bypass TypeScript declarations. Centralized schemas make
limits explicit and provide consistent failures across Bun, Node, and edge
consumers.

## Consequences

Invalid input fails before coordination state changes. Rule functions remain
trusted application code and are not represented as serializable Zod values.
