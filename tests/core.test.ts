import { describe, expect, test } from "bun:test";
import {
  InMemoryCoordinationStore,
  Matcher,
  MemoryPersistenceStore,
} from "../index.ts";

function setup() {
  let now = 1_000;
  const clock = () => now;
  const store = new InMemoryCoordinationStore();
  const persistence = new MemoryPersistenceStore();
  const matcher = new Matcher({
    store,
    persistence,
    clock,
    idGenerator: (() => {
      let id = 0;
      return () => `id-${++id}`;
    })(),
    random: () => 0,
  });
  return { matcher, store, persistence, clock, advance: (ms: number) => (now += ms) };
}

async function join(matcher: Matcher, id: string, preferences = {}) {
  await matcher.register({ id, preferences });
  await matcher.enqueue(id, ["chat"]);
}

describe("in-memory matchmaking", () => {
  test("matches compatible participants and persists the match", async () => {
    const { matcher, persistence } = setup();
    await join(matcher, "a", { language: "en" });
    await join(matcher, "b", { language: "en" });

    const result = await matcher.findMatch({
      requesterId: "a",
      poolKey: "chat",
      compatibility: (requester, candidate) =>
        requester.preferences.language === candidate.preferences.language,
    });

    expect(result.status).toBe("matched");
    if (result.status === "matched") {
      expect(result.claim.participants.map((p) => p.id)).toEqual(["a", "b"]);
      expect((await persistence.getMatch(result.claim.matchId))?.status).toBe("active");
      expect((await matcher.getParticipant("a"))?.state).toBe("matched");
    }
  });

  test("prevents concurrent requests from claiming a participant twice", async () => {
    const { matcher } = setup();
    await join(matcher, "a");
    await join(matcher, "b");
    await join(matcher, "c");

    const results = await Promise.all([
      matcher.findMatch({ requesterId: "a", poolKey: "chat" }),
      matcher.findMatch({ requesterId: "c", poolKey: "chat" }),
    ]);

    expect(results.filter((result) => result.status === "matched")).toHaveLength(1);
    expect(results.filter((result) => result.status === "none")).toHaveLength(1);
  });

  test("retries with an idempotency key return the original claim", async () => {
    const { matcher } = setup();
    await join(matcher, "a");
    await join(matcher, "b");
    const first = await matcher.findMatch({ requesterId: "a", poolKey: "chat", idempotencyKey: "request-1" });
    const second = await matcher.findMatch({ requesterId: "a", poolKey: "chat", idempotencyKey: "request-1" });
    expect(second).toEqual(first);
  });

  test("excludes skipped participants and supports best score", async () => {
    const { matcher } = setup();
    await join(matcher, "a");
    await join(matcher, "b", { score: 1 });
    await join(matcher, "c", { score: 10 });
    const result = await matcher.findMatch({
      requesterId: "a",
      poolKey: "chat",
      excludeIds: ["c"],
      score: (_requester, candidate) => Number(candidate.preferences.score),
      strategy: { type: "best-score" },
    });
    expect(result.status).toBe("matched");
    if (result.status === "matched") expect(result.claim.participants.map((p) => p.id)).toContain("b");
  });

  test("expires reserved claims and requeues participants", async () => {
    const { store, clock, advance } = setup();
    const matcher = new Matcher({
      store,
      clock,
      idGenerator: (() => {
        let id = 0;
        return () => `expiry-${++id}`;
      })(),
    });
    await join(matcher, "a");
    await join(matcher, "b");
    const result = await matcher.findMatch({ requesterId: "a", poolKey: "chat", claimTtlMs: 1_000 });
    expect(result.status).toBe("matched");
    advance(1_001);
    const expired = await matcher.recover();
    expect(expired).toHaveLength(1);
    expect(await matcher.getQueueDepth("chat")).toBe(2);
  });

  test("validates participant and matchmaking inputs with Zod", async () => {
    const { matcher } = setup();
    await expect(matcher.register({ id: "", preferences: {} })).rejects.toThrow();
    await expect(
      matcher.register({
        id: "oversized",
        preferences: { payload: "x".repeat(9 * 1024) },
      }),
    ).rejects.toThrow("8 KiB");
    await expect(matcher.findMatch({ requesterId: "a", poolKey: "chat", scanLimit: 0 })).rejects.toThrow();
  });
});
