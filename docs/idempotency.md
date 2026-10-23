# Idempotency

Pass `idempotencyKey` to `findMatch()` (or `atomicClaim()`) when a request may be
retried, for example after a timeout or a crashed worker. Both coordination
stores implement the same rules.

## What a key records

A key is recorded only when a claim is made. It points at that claim and
stores the request fingerprint. Replays read the live claim, so they always see
its current state.

`none`, `stale`, and `error` outcomes are not recorded. Retrying them runs the
search again and may find a match.

## Retry outcomes

| State of the original claim                | Retry with the same key and request                     |
| ------------------------------------------ | ------------------------------------------------------- |
| `reserved` (in flight or crashed)          | Same claim, `replayed: true`, state `reserved`          |
| `committed`                                | Same claim, `replayed: true`, state `committed`         |
| `rolled_back`                              | Key is freed; a new search runs and may match           |
| `expired` (recovered without a durable match) | Key is freed; a new search runs and may match        |
| Claim record gone (retention elapsed)      | Key is freed; a new search runs                         |

`Matcher.findMatch()` finishes a replayed `reserved` claim. It saves the match
again by `matchId` and commits. Both steps are idempotent, so this is safe
whether the first attempt crashed or is still running. A raw `atomicClaim()`
caller should do the same: complete or release the replayed claim.

A recovery pass that finds a durable match commits the claim, so a later retry
replays it as `committed`.

## Request fingerprints

While the original claim is `reserved` or `committed`, a key may only be
reused for the same request. A different request returns `status: "error"`
with `idempotency key reused with different request`.

The fingerprint covers `requesterId`, `poolKey`, `scanLimit`, `excludeIds`,
`strategy`, `filter`, and `maxHeartbeatAgeMs`. `Matcher` also records whether
compatibility, score, and relaxation callbacks were supplied. It does not cover
`cursor`, `claimId`, `matchId`, or `now`, so one key can span every page of a
cursor search. If `excludeIds` changes between attempts (for example, adding a
candidate that turned out to be unusable), that is a new request and needs a
new key, such as `${requestId}:${attempt}`.

## Lifetimes

| Record                     | Redis TTL                                       |
| -------------------------- | ----------------------------------------------- |
| Key for a reserved claim   | Claim TTL + `claimRetentionSeconds`             |
| Key for a committed claim  | `claimRetentionSeconds` from the commit         |

After that, the key is gone and a retry runs a new search. In-memory keys live
for the life of the process.

`Matcher` clears the key itself when `saveMatch()` fails and the claim is rolled
back.
