# ADR-0001: Pair-only v1 scope

- Status: Accepted
- Date: 2026-09-18

## Decision

Version one supports two-participant matches only. Groups, capacity assignment,
and multi-party sessions are deferred.

## Rationale

Pair matching establishes the claim, retry, expiry, and recovery semantics
without multiplying reservation complexity. The interfaces can later support
group claims without forcing group behavior into the first release.

## Consequences

The current `MatchClaim` always contains exactly two participants. Consumers
needing groups must compose their own workflow until a later adapter is shipped.
