# Concepts

## Participant

A participant is an entity identified by a stable `id`. Preferences are opaque
JSON data owned by the application. MatchForge stores a bounded snapshot in a
claim and does not require PII.

## Pool

A pool is an isolation boundary such as `chat:en` or `support:tier2`. Enqueueing
places a participant in one or more pools. A claim always belongs to one pool.

## Claim and match

A claim is a short-lived reservation made by the coordination store. A match is
the durable application record created after a claim succeeds. Claims are
`reserved`, then `committed`, `rolled_back`, or `expired`.

## Versions

Participants carry a monotonically increasing version. Waiting index entries and
recovery operations compare versions before changing state. This prevents an
old recovery operation from overwriting newer participant activity.

## State flow

`created -> waiting -> claimed -> matched`

Claims can return participants to `waiting` through rollback or expiry. A
participant can also leave the queue through `dequeue()`.
