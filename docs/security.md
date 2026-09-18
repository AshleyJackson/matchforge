# Security and privacy

MatchForge does not authenticate callers. Applications must authorize every
matcher command before invoking it.

Preferences are treated as potentially sensitive. They are not logged by the
core and are limited to 8 KiB of JSON-serializable data. Do not place secrets,
tokens, or credentials in preferences or metadata.

Apply application-level rate limits to registration, enqueue, heartbeat, and
matching commands. Limit pool count and exclusion-list size as appropriate for
the deployment.
