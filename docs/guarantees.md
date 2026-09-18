# Guarantees

| Capability                      | In-memory adapter      | Distributed adapter                                  |
| ------------------------------- | ---------------------- | ---------------------------------------------------- |
| Atomic claim within one process | Yes                    | Required                                             |
| Horizontal safety               | No                     | Redis/Lua claim path                                 |
| Waiting-state durability        | No                     | Adapter-dependent                                    |
| Claim expiry and version checks | Yes                    | Required                                             |
| Durable match records           | Via `PersistenceStore` | Via application store                                |
| Exactly-once practical pairing  | Single process only    | Depends on atomic adapter and idempotent persistence |

MatchForge does not provide authentication, authorization, transport delivery,
or a globally consistent multi-region queue.
