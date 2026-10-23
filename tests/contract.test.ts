import { afterAll, describe, expect, test } from "bun:test";
import { randomUUID } from "node:crypto";
import { z } from "zod";
import {
  InMemoryCoordinationStore,
  ParticipantSchema,
  RedisCoordinationStore,
  type AtomicClaimInputSchema,
} from "../index.ts";
import {
  deleteNamespace,
  integrationBackends,
  type IntegrationRedis,
} from "./support/redis.ts";

type Participant = z.infer<typeof ParticipantSchema>;
type ClaimInput = z.infer<typeof AtomicClaimInputSchema>;
type Store = InMemoryCoordinationStore | RedisCoordinationStore;

interface Harness {
  store: Store;
  /** Present for Redis backends, for assertions on raw keys. */
  redis?: IntegrationRedis;
  keyPrefix?: string;
  cleanup(): Promise<void>;
}

const NOW = Date.now();

const sharesInterest = {
  op: "overlaps" as const,
  left: { source: "requester" as const, path: ["interests"] },
  right: { source: "candidate" as const, path: ["interests"] },
};

function participant(id: string, overrides: Partial<Participant> = {}): Participant {
  return {
    id,
    preferences: { interests: ["games"] },
    version: 1,
    state: "waiting",
    joinedAt: NOW,
    lastHeartbeatAt: NOW,
    pools: ["chat"],
    ...overrides,
  };
}

async function add(store: Store, ...values: Participant[]) {
  await Promise.all(
    values.map(async (value) => {
      await store.putParticipant(value);
      await store.addToWaitingIndex({
        participantId: value.id,
        poolKey: value.pools[0] ?? "chat",
        joinedAt: value.joinedAt,
        version: value.version,
      });
    }),
  );
}

let sequence = 0;
function claimInput(overrides: Partial<ClaimInput> = {}): ClaimInput {
  sequence += 1;
  return {
    requesterId: "a",
    poolKey: "chat",
    now: NOW,
    scanLimit: 50,
    excludeIds: [],
    strategy: { type: "fifo" },
    filter: sharesInterest,
    claimTtlMs: 5_000,
    claimId: `claim-${sequence}`,
    matchId: `match-${sequence}`,
    random: () => 0,
    ...overrides,
  };
}

function partnerOf(result: Awaited<ReturnType<Store["atomicClaim"]>>) {
  return result.status === "matched" ? result.claim.participants[1]!.id : null;
}

const harnesses: Array<{ name: string; create(): Harness }> = [
  {
    name: "InMemoryCoordinationStore",
    create: () => ({
      store: new InMemoryCoordinationStore(),
      cleanup: async () => undefined,
    }),
  },
  ...integrationBackends().map((backend) => ({
    name: `RedisCoordinationStore on ${backend.name}`,
    create: (): Harness => {
      const redis = backend.create();
      const namespace = `matchforge:test:${randomUUID()}`;
      return {
        store: new RedisCoordinationStore({
          redis,
          namespace,
          participantTtlSeconds: 120,
          claimTtlSeconds: 5,
          claimRetentionSeconds: 30,
        }),
        redis,
        keyPrefix: `${namespace}:{${namespace}}:`,
        cleanup: async () => {
          await deleteNamespace(redis, namespace);
          redis.close();
        },
      };
    },
  })),
];

for (const { name, create } of harnesses) {
  describe(`coordination contract: ${name}`, () => {
    const open: Harness[] = [];
    const setup = () => {
      const harness = create();
      open.push(harness);
      return harness;
    };
    afterAll(async () => {
      await Promise.all(open.map((harness) => harness.cleanup()));
    });

    describe("scanning", () => {
      test("continues past the first scan window with a cursor", async () => {
        const { store } = setup();
        await add(
          store,
          participant("a", { joinedAt: NOW - 1_000 }),
          ...Array.from({ length: 120 }, (_, index) =>
            participant(`x${index}`, {
              preferences: { interests: ["cooking"] },
              joinedAt: NOW + index,
            }),
          ),
          participant("b", { joinedAt: NOW + 500 }),
        );

        const first = await store.atomicClaim(claimInput({ scanLimit: 100 }));
        expect(first.status).toBe("none");
        if (first.status !== "none") return;
        expect(first.cursor).toBeString();

        const second = await store.atomicClaim(
          claimInput({ scanLimit: 100, cursor: first.cursor }),
        );
        expect(partnerOf(second)).toBe("b");
      });

      test("omits the cursor once the pool is exhausted", async () => {
        const { store } = setup();
        await add(
          store,
          participant("a", { joinedAt: NOW - 1 }),
          participant("x", { preferences: { interests: ["cooking"] } }),
        );
        const result = await store.atomicClaim(claimInput());
        expect(result.status).toBe("none");
        if (result.status === "none") expect(result.cursor).toBeUndefined();
      });

      test("does not spend the scan budget on excluded participants", async () => {
        const { store } = setup();
        const excluded = Array.from({ length: 20 }, (_, index) => `x${index}`);
        await add(
          store,
          participant("a", { joinedAt: NOW - 1_000 }),
          ...excluded.map((id, index) => participant(id, { joinedAt: NOW + index })),
          participant("b", { joinedAt: NOW + 500 }),
        );

        const result = await store.atomicClaim(
          claimInput({ scanLimit: 1, excludeIds: excluded }),
        );
        expect(partnerOf(result)).toBe("b");
      });

      test("keeps candidates that share a join time across pages", async () => {
        const { store } = setup();
        await add(
          store,
          participant("a", { joinedAt: NOW - 1 }),
          participant("m1", { preferences: { interests: ["cooking"] } }),
          participant("m2", { preferences: { interests: ["cooking"] } }),
          participant("m3"),
        );
        const first = await store.atomicClaim(claimInput({ scanLimit: 2 }));
        expect(first.status).toBe("none");
        if (first.status !== "none") return;
        const second = await store.atomicClaim(
          claimInput({ scanLimit: 2, cursor: first.cursor }),
        );
        expect(partnerOf(second)).toBe("m3");
      });
    });

    describe("exclusions and presence", () => {
      test("skips a candidate who excluded the requester", async () => {
        const { store } = setup();
        await add(
          store,
          participant("a", { joinedAt: NOW - 2 }),
          participant("b", { joinedAt: NOW - 1, exclusions: { a: NOW + 60_000 } }),
          participant("c"),
        );
        expect(partnerOf(await store.atomicClaim(claimInput()))).toBe("c");
      });

      test("skips a candidate the requester excluded", async () => {
        const { store } = setup();
        await add(
          store,
          participant("a", { joinedAt: NOW - 1, exclusions: { b: NOW + 60_000 } }),
          participant("b"),
        );
        expect((await store.atomicClaim(claimInput())).status).toBe("none");
      });

      test("ignores exclusions that have ended", async () => {
        const { store } = setup();
        await add(
          store,
          participant("a", { joinedAt: NOW - 1, exclusions: { b: NOW - 1 } }),
          participant("b"),
        );
        expect(partnerOf(await store.atomicClaim(claimInput()))).toBe("b");
      });

      test("skips candidates whose heartbeat is older than maxHeartbeatAgeMs", async () => {
        const { store } = setup();
        await add(
          store,
          participant("a", { joinedAt: NOW - 2 }),
          participant("b", { joinedAt: NOW - 1, lastHeartbeatAt: NOW - 60_000 }),
          participant("c"),
        );
        expect(
          partnerOf(await store.atomicClaim(claimInput({ maxHeartbeatAgeMs: 30_000 }))),
        ).toBe("c");
      });

      test("does not check presence without maxHeartbeatAgeMs", async () => {
        const { store } = setup();
        await add(
          store,
          participant("a", { joinedAt: NOW - 2 }),
          participant("b", { joinedAt: NOW - 1, lastHeartbeatAt: NOW - 60_000 }),
        );
        expect(partnerOf(await store.atomicClaim(claimInput()))).toBe("b");
      });
    });

    describe("syncParticipant", () => {
      const desired = (overrides: Partial<Parameters<Store["syncParticipant"]>[0]> = {}) => ({
        id: "a",
        preferences: { interests: [] as string[] },
        state: "waiting" as const,
        pools: ["chat"],
        joinedAt: NOW,
        lastHeartbeatAt: NOW,
        ...overrides,
      });

      test("creates a participant and indexes it", async () => {
        const { store } = setup();
        const result = await store.syncParticipant(desired());
        expect(result.status).toBe("created");
        expect(result.participant?.version).toBe(1);
        expect(await store.getQueueDepth("chat")).toBe(1);
        expect((await store.getParticipant("a"))?.preferences).toEqual({ interests: [] });
      });

      test("moves pools and removes non-waiting participants from the index", async () => {
        const { store } = setup();
        await store.syncParticipant(desired());
        const moved = await store.syncParticipant(desired({ pools: ["video"] }));
        expect(moved).toMatchObject({ status: "updated", participant: { version: 2 } });
        expect(await store.getQueueDepth("chat")).toBe(0);
        expect(await store.getQueueDepth("video")).toBe(1);

        const left = await store.syncParticipant(desired({ pools: ["video"], state: "left" }));
        expect(left.status).toBe("updated");
        expect(await store.getQueueDepth("video")).toBe(0);
      });

      test("rejects a mismatched expectedVersion without writing", async () => {
        const { store } = setup();
        expect((await store.syncParticipant(desired({ expectedVersion: 0 }))).status).toBe(
          "created",
        );
        const conflict = await store.syncParticipant(
          desired({ expectedVersion: 0, pools: ["video"] }),
        );
        expect(conflict).toMatchObject({ status: "conflict", participant: { version: 1 } });
        expect(await store.getQueueDepth("video")).toBe(0);
        expect(
          (await store.syncParticipant(desired({ expectedVersion: 1 }))).status,
        ).toBe("updated");
        expect(
          (await store.syncParticipant({ ...desired(), id: "z", expectedVersion: 3 })),
        ).toEqual({ status: "conflict", participant: null });
      });

      test("skips writes whose revision is not newer", async () => {
        const { store } = setup();
        await store.syncParticipant(desired({ revision: 5 }));
        const stale = await store.syncParticipant(desired({ revision: 5, state: "left" }));
        expect(stale).toMatchObject({ status: "stale", participant: { syncRevision: 5 } });
        expect(await store.getQueueDepth("chat")).toBe(1);

        await store.syncParticipant(desired({ revision: 6 }));
        const unversioned = await store.syncParticipant(desired());
        expect(unversioned).toMatchObject({
          status: "updated",
          participant: { syncRevision: 6, version: 3 },
        });
      });

      test("never overwrites a participant held by a claim", async () => {
        const { store } = setup();
        await store.syncParticipant(desired({ preferences: { interests: ["games"] } }));
        await store.syncParticipant({
          ...desired({ preferences: { interests: ["games"] }, joinedAt: NOW + 1 }),
          id: "b",
        });
        const claim = await store.atomicClaim(claimInput());
        expect(claim.status).toBe("matched");

        const result = await store.syncParticipant(desired({ state: "left" }));
        expect(result).toMatchObject({ status: "claimed", participant: { state: "claimed" } });
        expect((await store.getParticipant("a"))?.state).toBe("claimed");
      });

      test("stores exclusions that later claims honour", async () => {
        const { store } = setup();
        await store.syncParticipant(desired({ preferences: { interests: ["games"] } }));
        await store.syncParticipant({
          ...desired({ preferences: { interests: ["games"] }, joinedAt: NOW + 1 }),
          id: "b",
          exclusions: { a: NOW + 60_000 },
        });
        expect((await store.atomicClaim(claimInput())).status).toBe("none");
      });
    });

    describe("idempotency", () => {
      test("replays a live claim and rejects a different request", async () => {
        const { store } = setup();
        await add(store, participant("a", { joinedAt: NOW - 1 }), participant("b"));
        const first = await store.atomicClaim(claimInput({ idempotencyKey: "k" }));
        expect(first.status).toBe("matched");
        if (first.status !== "matched") return;
        expect(first.replayed).toBeUndefined();

        const retry = await store.atomicClaim(claimInput({ idempotencyKey: "k" }));
        expect(retry).toMatchObject({
          status: "matched",
          replayed: true,
          claim: { claimId: first.claim.claimId, state: "reserved" },
        });

        const withCursor = await store.atomicClaim(
          claimInput({ idempotencyKey: "k", cursor: `${NOW}:zzz` }),
        );
        expect(withCursor).toMatchObject({ status: "matched", replayed: true });

        const different = await store.atomicClaim(
          claimInput({ idempotencyKey: "k", excludeIds: ["c"] }),
        );
        expect(different.status).toBe("error");

        expect(await store.commitClaim(first.claim.claimId)).toBe(true);
        expect(await store.commitClaim(first.claim.claimId)).toBe(true);
        const afterCommit = await store.atomicClaim(claimInput({ idempotencyKey: "k" }));
        expect(afterCommit).toMatchObject({
          status: "matched",
          replayed: true,
          claim: { claimId: first.claim.claimId, state: "committed" },
        });
      });

      test("frees the key when the claim is rolled back", async () => {
        const { store } = setup();
        await add(store, participant("a", { joinedAt: NOW - 1 }), participant("b"));
        const first = await store.atomicClaim(claimInput({ idempotencyKey: "k" }));
        if (first.status !== "matched") throw new Error("expected a claim");
        expect(await store.rollbackClaim(first.claim.claimId)).toBe(true);

        const retry = await store.atomicClaim(
          claimInput({ idempotencyKey: "k", excludeIds: ["c"] }),
        );
        expect(retry.status).toBe("matched");
        if (retry.status !== "matched") return;
        expect(retry.replayed).toBeUndefined();
        expect(retry.claim.claimId).not.toBe(first.claim.claimId);
      });

      test("frees the key when recovery expires the claim", async () => {
        const { store } = setup();
        await add(store, participant("a", { joinedAt: NOW - 1 }), participant("b"));
        const first = await store.atomicClaim(claimInput({ idempotencyKey: "k" }));
        if (first.status !== "matched") throw new Error("expected a claim");
        const expired = await store.recoverExpiredClaims(NOW + 60_000);
        expect(expired.map((claim) => claim.state)).toEqual(["expired"]);

        const retry = await store.atomicClaim(claimInput({ idempotencyKey: "k" }));
        expect(retry.status).toBe("matched");
        if (retry.status === "matched")
          expect(retry.claim.claimId).not.toBe(first.claim.claimId);
      });

      test("does not record none or stale outcomes", async () => {
        const { store } = setup();
        await add(store, participant("a", { joinedAt: NOW - 1 }));
        expect((await store.atomicClaim(claimInput({ idempotencyKey: "k" }))).status).toBe(
          "none",
        );
        await add(store, participant("b"));
        expect(
          partnerOf(await store.atomicClaim(claimInput({ idempotencyKey: "k" }))),
        ).toBe("b");
      });
    });

    describe("recovery", () => {
      async function claimPairs(store: Store, count: number) {
        const ids: string[] = [];
        for (let index = 0; index < count; index += 1) {
          const requester = `r${index}`;
          await add(
            store,
            participant(requester, { joinedAt: NOW + index * 10 }),
            participant(`c${index}`, { joinedAt: NOW + index * 10 + 1 }),
          );
          const result = await store.atomicClaim(
            claimInput({ requesterId: requester, claimTtlMs: 1_000 + index * 1_000 }),
          );
          if (result.status !== "matched") throw new Error("expected a claim");
          ids.push(result.claim.claimId);
        }
        return ids;
      }

      test("recovers only expired claims, oldest first, in bounded batches", async () => {
        const { store } = setup();
        const ids = await claimPairs(store, 3);

        expect(await store.recoverExpiredClaims(NOW + 500)).toHaveLength(0);
        const firstPass = await store.recoverExpiredClaims(NOW + 60_000, undefined, {
          limit: 2,
        });
        expect(firstPass.map((claim) => claim.claimId)).toEqual(ids.slice(0, 2));
        const secondPass = await store.recoverExpiredClaims(NOW + 60_000, undefined, {
          limit: 2,
        });
        expect(secondPass.map((claim) => claim.claimId)).toEqual(ids.slice(2));
        expect(await store.recoverExpiredClaims(NOW + 60_000)).toHaveLength(0);
        expect((await store.getParticipant("r0"))?.state).toBe("waiting");
      });

      test("skips committed claims and completes claims backed by a durable match", async () => {
        const { store } = setup();
        const [committed, durable] = await claimPairs(store, 2);
        expect(await store.commitClaim(committed!)).toBe(true);

        await store.recoverExpiredClaims(NOW + 60_000, async () => true);
        expect((await store.getClaim(durable!))?.state).toBe("committed");
        expect((await store.getParticipant("r1"))?.state).toBe("matched");
        expect(await store.recoverExpiredClaims(NOW + 60_000)).toHaveLength(0);
      });
    });
  });
}

for (const backend of integrationBackends()) {
  describe(`Redis claim index on ${backend.name}`, () => {
    test("keeps only reserved claims in the expiry index", async () => {
      const redis = backend.create();
      const namespace = `matchforge:test:${randomUUID()}`;
      const prefix = `${namespace}:{${namespace}}:`;
      const store = new RedisCoordinationStore({ redis, namespace, claimTtlSeconds: 5 });
      try {
        await add(store, participant("a", { joinedAt: NOW - 1 }), participant("b"));
        const result = await store.atomicClaim(claimInput());
        if (result.status !== "matched") throw new Error("expected a claim");
        expect(await redis.zrange(`${prefix}claim-expiry`, 0, -1)).toEqual([
          result.claim.claimId,
        ]);
        expect(await store.commitClaim(result.claim.claimId)).toBe(true);
        expect(await redis.zrange(`${prefix}claim-expiry`, 0, -1)).toEqual([]);
      } finally {
        await deleteNamespace(redis, namespace);
        redis.close();
      }
    });

    test("drains claims from the legacy unsorted claim set", async () => {
      const redis = backend.create();
      const namespace = `matchforge:test:${randomUUID()}`;
      const prefix = `${namespace}:{${namespace}}:`;
      const store = new RedisCoordinationStore({ redis, namespace, claimTtlSeconds: 5 });
      try {
        await add(store, participant("a", { joinedAt: NOW - 1 }), participant("b"));
        const result = await store.atomicClaim(claimInput());
        if (result.status !== "matched") throw new Error("expected a claim");
        await redis.zrem(`${prefix}claim-expiry`, result.claim.claimId);
        await redis.sadd(`${prefix}claims`, result.claim.claimId, "missing-claim");

        const recovered = await store.recoverExpiredClaims(NOW + 60_000);
        expect(recovered.map((claim) => claim.claimId)).toEqual([result.claim.claimId]);
        expect(await redis.smembers(`${prefix}claims`)).toEqual([]);
        expect((await store.getParticipant("b"))?.state).toBe("waiting");
      } finally {
        await deleteNamespace(redis, namespace);
        redis.close();
      }
    });
  });
}
