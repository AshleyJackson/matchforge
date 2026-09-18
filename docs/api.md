# API quick reference

The root package re-exports the public API from focused modules. Consumers do
not need to import internal files.

```ts
import { InMemoryCoordinationStore, Matcher } from "matchforge";

const matcher = new Matcher({
  store: new InMemoryCoordinationStore(),
});

await matcher.register({
  id: "alice",
  preferences: { language: "en" },
});
await matcher.enqueue("alice", ["chat:en"]);
```

Public participant, preference, pool, and match-request inputs are validated with
Zod. Preferences must be JSON-serializable and no larger than 8 KiB.

Call `findMatch()` with a compatibility predicate for hard constraints or a score
function for ranking:

```ts
const result = await matcher.findMatch({
  requesterId: "alice",
  poolKey: "chat:en",
  strategy: { type: "best-score" },
  compatibility: (a, b) => a.preferences.language === b.preferences.language,
  score: (a, b) => Number(b.preferences.interestsInCommon ?? 0),
});
```

Use a `PersistenceStore` in production. The coordination store owns ephemeral
waiting and claims; the persistence store owns durable matches.
