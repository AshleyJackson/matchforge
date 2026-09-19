# ADR 0008: Zod-only runtime contracts

- Status: Accepted
- Date: 2026-09-19

## Context

MatchForge previously maintained parallel TypeScript `type` and `interface`
declarations beside its Zod schemas. That duplicated the public contract,
allowed compile-time and runtime definitions to drift, and made serialized
adapter boundaries harder to audit.

## Decision

All public data and behavioral contracts are defined as native Zod schemas.
Implementations validate inputs at their boundaries and derive annotations
directly from the relevant schema with `z.infer`, without a separate
`types.ts` module or handwritten TypeScript interfaces.

Function-valued fields use `z.function`, and object-shaped service contracts
use `z.object`. Schemas remain the source of truth for validation,
serialization, and TypeScript inference.

## Consequences

- Public consumers import schemas and runtime classes, not handwritten type
  declarations.
- Invalid adapter implementations can be rejected with `Schema.parse`.
- The generated declaration output is derived from Zod schemas.
- Function schemas are more verbose than an interface, but they make the
  callable contract explicit and inspectable at runtime.
- Recursive schemas may require a small recursive `z.ZodType` annotation to
  guide TypeScript's inference; this is schema metadata, not a parallel
  domain type.
