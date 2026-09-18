# Matching rules

`findMatch()` accepts application-provided functions:

- `compatibility` is a hard filter. Return `false` to reject a candidate.
- `score` returns a number for eligible candidates or `null` for rejection.
- `relax` progressively transforms preferences based on wait time.

Rules should be deterministic, side-effect free, and fast. They run inside the
coordination operation in the in-memory adapter. Do not perform network calls or
mutations from a rule.

Selection strategies are `fifo`, `random`, `best-score`, `weighted-random`, and
`custom`. Scans are bounded by `scanLimit`; the default is 100.
