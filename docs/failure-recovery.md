# Failure and recovery

## Persistence failure

When a configured `PersistenceStore` rejects `saveMatch()`, the matcher rolls
the reserved claim back and requeues both participants. The call returns an
`error` result.

## Process crash

An uncommitted claim remains reserved until its TTL. Call `recover()` from a
lightweight sweeper or before matching to expire claims and requeue participants
whose versions still match the claim. Each call handles at most `limit` claims
(default 100), oldest expiry first; call it again when a pass returns a full
batch.

A client retrying the crashed request with the same idempotency key gets the
reserved claim back and can finish it. See [Idempotency](idempotency.md).

## Durable write before commit

The durable write uses the claim's `matchId` and should be idempotent. If a
process crashes after the durable write but before commit, a production
reconciler should inspect the durable match and complete the claim rather than
creating a second match.

## Stale requests

Requests for participants that are no longer waiting return `stale`. This is
normal under retries and concurrent dequeue or match operations.
