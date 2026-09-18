# Versioning

The package follows semantic versioning.

The following are compatibility boundaries:

- exported TypeScript types and methods
- claim state and recovery semantics
- coordination-store behavior
- adapter key schemas and scripts once Redis ships

The in-memory adapter is a reference implementation, not a persistence format.
Distributed adapters must version their scripts and storage schemas. Breaking
claim or key changes require a major version or an explicit migration path.
