# Observability

The public `MetricsSink` interface is available for adapters and future matcher
instrumentation. Recommended metrics include:

- claims by status
- claim latency
- candidates scanned
- queue depth
- wait time
- expired and rolled-back claims
- persistence and adapter errors

Log claim IDs, match IDs, pool keys, and safe participant identifiers. Never log
full preference objects by default.
