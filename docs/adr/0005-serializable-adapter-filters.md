# ADR-0005: Serializable adapter-side filter contract

- Status: Accepted
- Date: 2026-09-18

## Decision

Distributed coordination adapters require a Zod-validated, JSON-serializable
filter expression. The first expression language supports `all`, `any`, `not`,
and `overlaps`. Filters address requester or candidate preference values through
generic JSON paths and may compare against literal values.

Redis claims remain FIFO. Arbitrary TypeScript compatibility, scoring, and
relaxation callbacks are rejected by the Redis adapter because they cannot
execute inside an atomic Lua script.

## Rationale

The contract must remain agnostic to the calling domain. MatchForge should not
know whether a path represents interests, language, skill, media mode, or a
support attribute. A generic AST preserves that neutrality while allowing Redis
to evaluate a bounded expression without network calls or application code.

## Consequences

The memory adapter can continue to support trusted application callbacks.
Distributed callers must express hard eligibility through the serializable AST
or use a future two-phase worker API. Adding an operator requires updating the
Zod schema, memory evaluator, adapter scripts, and contract tests together.
