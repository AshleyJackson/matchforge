# Scaling

Use bounded scans. Increase `scanLimit` only after measuring latency and
candidate quality. For large queues, partition pools by coarse attributes such
as region, language, or skill band.

The in-memory adapter is process-local and must not be shared between horizontally
scaled instances. Use a distributed coordination adapter when multiple workers
can claim the same pool.

Avoid putting large preference objects in queue indexes. MatchForge keeps queue
entries to participant IDs, timestamps, versions, and optional score hints.
