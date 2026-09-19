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
  return {
    matcher,
    store,
    persistence,
    clock,
    advance: (ms: number) => (now += ms),
  };
}

async function join(matcher: Matcher, id: string, preferences = {}) {
  await matcher.register({ id, preferences });
  await matcher.enqueue(id, ["chat"]);
}

class FailingPersistenceStore extends MemoryPersistenceStore {
  override async saveMatch(): Promise<void> {
    throw new Error("persistence unavailable");
  }
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
      expect((await persistence.getMatch(result.claim.matchId))?.status).toBe(
        "active",
      );
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

    expect(
      results.filter((result) => result.status === "matched"),
    ).toHaveLength(1);
    expect(results.filter((result) => result.status === "none")).toHaveLength(
      1,
    );
  });

  test("retries with an idempotency key return the original claim", async () => {
    const { matcher } = setup();
    await join(matcher, "a");
    await join(matcher, "b");
    const first = await matcher.findMatch({
      requesterId: "a",
      poolKey: "chat",
      idempotencyKey: "request-1",
    });
    const second = await matcher.findMatch({
      requesterId: "a",
      poolKey: "chat",
      idempotencyKey: "request-1",
    });
    expect(second.status).toBe("matched");
    if (first.status === "matched" && second.status === "matched") {
      expect(second.claim.claimId).toBe(first.claim.claimId);
      expect(second.claim.matchId).toBe(first.claim.matchId);
      expect(second.claim.participants).toEqual(first.claim.participants);
      expect(second.claim.state).toBe("committed");
    }
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
    if (result.status === "matched")
      expect(result.claim.participants.map((p) => p.id)).toContain("b");
  });

  test("expires reserved claims and requeues participants", async () => {
    const { store, advance } = setup();
    await store.putParticipant({
      id: "a",
      preferences: {},
      version: 1,
      state: "waiting",
      joinedAt: 1_000,
      lastHeartbeatAt: 1_000,
      pools: ["chat"],
    });
    await store.putParticipant({
      id: "b",
      preferences: {},
      version: 1,
      state: "waiting",
      joinedAt: 1_000,
      lastHeartbeatAt: 1_000,
      pools: ["chat"],
    });
    await store.addToWaitingIndex({
      participantId: "a",
      poolKey: "chat",
      joinedAt: 1_000,
      version: 1,
    });
    await store.addToWaitingIndex({
      participantId: "b",
      poolKey: "chat",
      joinedAt: 1_000,
      version: 1,
    });
    const result = await store.atomicClaim({
      requesterId: "a",
      poolKey: "chat",
      now: 1_000,
      scanLimit: 10,
      excludeIds: [],
      strategy: { type: "fifo" },
      claimTtlMs: 1_000,
      claimId: "expiry-claim",
      matchId: "expiry-match",
      random: () => 0,
    });
    expect(result.status).toBe("matched");
    advance(1_001);
    const expired = await store.recoverExpiredClaims(2_001);
    expect(expired).toHaveLength(1);
    expect(await store.getQueueDepth("chat")).toBe(2);
  });

  test("commits matches without a persistence store", async () => {
    const { store } = setup();
    const matcher = new Matcher({ store });
    await join(matcher, "a");
    await join(matcher, "b");

    const result = await matcher.findMatch({
      requesterId: "a",
      poolKey: "chat",
    });

    expect(result.status).toBe("matched");
    if (result.status === "matched") {
      expect(result.claim.state).toBe("committed");
      expect((await matcher.getParticipant("a"))?.state).toBe("matched");
      expect((await matcher.getClaim(result.claim.claimId))?.state).toBe(
        "committed",
      );
    }
  });

  test("rolls back a failed durable write and clears its idempotency key", async () => {
    const { store } = setup();
    const matcher = new Matcher({
      store,
      persistence: new FailingPersistenceStore(),
    });
    await join(matcher, "a");
    await join(matcher, "b");

    const result = await matcher.findMatch({
      requesterId: "a",
      poolKey: "chat",
      idempotencyKey: "failed-write",
    });

    expect(result.status).toBe("error");
    expect(await matcher.getQueueDepth("chat")).toBe(2);
    expect((await matcher.getParticipant("a"))?.state).toBe("waiting");
    expect((await matcher.getParticipant("b"))?.state).toBe("waiting");
  });

  test("reconciles a durable match and updates idempotent retries", async () => {
    const { matcher, advance } = setup();
    await join(matcher, "a");
    await join(matcher, "b");

    const first = await matcher.findMatch({
      requesterId: "a",
      poolKey: "chat",
      idempotencyKey: "reconcile-me",
      claimTtlMs: 1_000,
    });
    expect(first.status).toBe("matched");
    advance(1_001);

    const recovered = await matcher.recover();
    expect(recovered).toHaveLength(0);
    expect((await matcher.getParticipant("a"))?.state).toBe("matched");

    const retry = await matcher.findMatch({
      requesterId: "a",
      poolKey: "chat",
      idempotencyKey: "reconcile-me",
    });
    expect(retry.status).toBe("matched");
    if (retry.status === "matched") expect(retry.claim.state).toBe("committed");
  });

  test("does not partially commit when a participant version conflicts", async () => {
    const { matcher, store } = setup();
    await join(matcher, "a");
    await join(matcher, "b");
    const result = await store.atomicClaim({
      requesterId: "a",
      poolKey: "chat",
      now: 1_000,
      scanLimit: 10,
      excludeIds: [],
      strategy: { type: "fifo" },
      claimTtlMs: 20_000,
      claimId: "conflict-claim",
      matchId: "conflict-match",
      random: () => 0,
    });
    expect(result.status).toBe("matched");
    await matcher.updatePreferences("b", { changed: true });

    expect(await matcher.commitClaim("conflict-claim").catch(() => false)).toBe(
      false,
    );
    expect((await matcher.getParticipant("a"))?.state).toBe("claimed");
    expect((await matcher.getParticipant("b"))?.state).toBe("claimed");
  });

  test("reports heartbeat liveness against the configured clock", async () => {
    const { matcher, advance } = setup();
    await matcher.register({ id: "a", preferences: {} });

    expect(await matcher.isAlive("a")).toBe(true);
    advance(30_001);
    expect(await matcher.isAlive("a")).toBe(false);
  });

  test("validates participant and matchmaking inputs with Zod", async () => {
    const { matcher } = setup();
    await expect(
      matcher.register({ id: "", preferences: {} }),
    ).rejects.toThrow();
    await expect(
      matcher.register({
        id: "oversized",
        preferences: { payload: "x".repeat(9 * 1024) },
      }),
    ).rejects.toThrow("8 KiB");
    await expect(
      matcher.findMatch({ requesterId: "a", poolKey: "chat", scanLimit: 0 }),
    ).rejects.toThrow();
    await expect(
      matcher.findMatch({
        requesterId: "a",
        poolKey: "chat",
        filter: { op: "all", filters: [] },
      }),
    ).rejects.toThrow();
    await expect(matcher.enqueue("a", ["chat", "video"])).rejects.toThrow();
  });

  test("evaluates generic serializable filters without domain-specific rules", async () => {
    const { matcher } = setup();
    await join(matcher, "a", { interests: ["music", "games"] });
    await join(matcher, "b", { interests: ["cooking"] });
    await join(matcher, "c", { interests: ["games"] });

    const result = await matcher.findMatch({
      requesterId: "a",
      poolKey: "chat",
      filter: {
        op: "all",
        filters: [
          {
            op: "overlaps",
            left: { source: "requester", path: ["interests"] },
            right: { source: "candidate", path: ["interests"] },
          },
          {
            op: "not",
            filter: {
              op: "overlaps",
              left: { source: "candidate", path: ["interests"] },
              right: { literal: ["cooking"] },
            },
          },
        ],
      },
    });

    expect(result.status).toBe("matched");
    if (result.status === "matched") {
      expect(
        result.claim.participants.map((participant) => participant.id),
      ).toEqual(["a", "c"]);
    }
  });

  test("evaluates scalar serializable filters", async () => {
    const { matcher } = setup();
    await join(matcher, "a", { language: "en", tags: ["music", "games"] });
    await join(matcher, "b", { language: "fr", tags: ["games"] });
    await join(matcher, "c", { language: "en", tags: ["cooking"] });

    const result = await matcher.findMatch({
      requesterId: "a",
      poolKey: "chat",
      filter: {
        op: "all",
        filters: [
          {
            op: "equals",
            left: { source: "requester", path: ["language"] },
            right: { source: "candidate", path: ["language"] },
          },
          {
            op: "contains",
            left: { source: "candidate", path: ["tags"] },
            right: { literal: "cooking" },
          },
        ],
      },
    });

    expect(result.status).toBe("matched");
    if (result.status === "matched") {
      expect(result.claim.participants.map((participant) => participant.id)).toEqual(["a", "c"]);
    }
  });
});
