import { describe, expect, test } from "bun:test";
import { randomUUID } from "node:crypto";
import { z } from "zod";
import {
  RedisCoordinationStore,
  ParticipantSchema,
  buildMutualCompatibilityFilter,
} from "../index.ts";
import {
  deleteNamespace,
  integrationBackends,
  type IntegrationRedis,
} from "./support/redis.ts";

type Participant = z.infer<typeof ParticipantSchema>;

// Runs against the first configured real Redis (REDIS_URL, then Upstash).
const backend = integrationBackends()[0];
const integrationTest = backend ? test : test.skip;
const opened: IntegrationRedis[] = [];

function createRedis() {
  if (!backend)
    throw new Error("REDIS_URL or UPSTASH_URL and UPSTASH_TOKEN are required");
  const redis = backend.create();
  opened.push(redis);
  return redis;
}

function participant(
  id: string,
  interests: string[] = ["games"],
): Participant {
  return {
    id,
    preferences: { interests },
    version: 1,
    state: "waiting",
    joinedAt: Date.now(),
    lastHeartbeatAt: Date.now(),
    pools: ["chat"],
  };
}

function filter() {
  return {
    op: "overlaps" as const,
    left: { source: "requester" as const, path: ["interests"] },
    right: { source: "candidate" as const, path: ["interests"] },
  };
}

async function withStore(
  run: (
    store: RedisCoordinationStore,
    redis: IntegrationRedis,
    namespace: string,
  ) => Promise<void>,
) {
  const redis = createRedis();
  const namespace = `matchforge:test:${randomUUID()}`;
  const store = new RedisCoordinationStore({
    redis,
    namespace,
    participantTtlSeconds: 120,
    claimTtlSeconds: 2,
    claimRetentionSeconds: 30,
  });
  try {
    await run(store, redis, namespace);
  } finally {
    await deleteNamespace(redis, namespace);
    for (const client of opened.splice(0)) client.close();
  }
}

async function add(store: RedisCoordinationStore, value: Participant) {
  await store.putParticipant(value);
  await store.addToWaitingIndex({
    participantId: value.id,
    poolKey: "chat",
    joinedAt: value.joinedAt,
    version: value.version,
  });
}

describe(`Redis coordination on ${backend?.name ?? "real Redis"}`, () => {
  integrationTest(
    "claims, retries idempotently, and commits atomically",
    async () => {
      await withStore(async (store, redis, namespace) => {
        await add(store, participant("a"));
        await add(store, participant("b"));

        const input = {
          requesterId: "a",
          poolKey: "chat",
          now: Date.now(),
          scanLimit: 50,
          excludeIds: [],
          strategy: { type: "fifo" as const },
          filter: filter(),
          claimTtlMs: 2_000,
          claimId: "claim-1",
          matchId: "match-1",
          idempotencyKey: "request-1",
          random: () => 0,
        };
        const first = await store.atomicClaim(input);
        expect(first.status).toBe("matched");

        const retry = await store.atomicClaim({
          ...input,
          claimId: "claim-2",
          matchId: "match-2",
        });
        expect(retry.status).toBe("matched");
        if (first.status === "matched" && retry.status === "matched") {
          expect(retry.claim.claimId).toBe(first.claim.claimId);
        }

        const pendingClaims = await redis.zrange(
          `${namespace}:{${namespace}}:claim-expiry`,
          0,
          -1,
        );
        expect(pendingClaims).toContain("claim-1");
        expect(await store.commitClaim("claim-1")).toBe(true);
        expect((await store.getParticipant("a"))?.state).toBe("matched");
        expect((await store.getParticipant("b"))?.state).toBe("matched");
      });
    },
  );

  integrationTest(
    "applies recursive filters and skips incompatible candidates",
    async () => {
      await withStore(async (store) => {
        await add(store, participant("a", ["games"]));
        await add(store, participant("b", ["cooking"]));
        await add(store, participant("c", ["games"]));

        const result = await store.atomicClaim({
          requesterId: "a",
          poolKey: "chat",
          now: Date.now(),
          scanLimit: 50,
          excludeIds: [],
          strategy: { type: "fifo" },
          filter: {
            op: "all",
            filters: [
              filter(),
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
          claimTtlMs: 2_000,
          claimId: "claim-2",
          matchId: "match-2",
          random: () => 0,
        });

        expect(result.status).toBe("matched");
        if (result.status === "matched") {
          expect(result.claim.participants.map(({ id }) => id)).toEqual([
            "a",
            "c",
          ]);
        }
      });
    },
  );

  integrationTest(
    "applies symmetric wildcard and empty-collection compatibility in Redis",
    async () => {
      await withStore(async (store) => {
        const a = {
          ...participant("a", ["music"]),
          preferences: { interests: ["music"], category: "standard" },
        };
        const b = {
          ...participant("b", []),
          preferences: { interests: [], category: "any" },
        };
        await add(store, a);
        await add(store, b);

        const result = await store.atomicClaim({
          requesterId: "a",
          poolKey: "chat",
          now: Date.now(),
          scanLimit: 50,
          excludeIds: [],
          strategy: { type: "fifo" as const },
          filter: buildMutualCompatibilityFilter({
            rules: [
              {
                mode: "equals",
                requesterPath: ["category"],
                candidatePath: ["category"],
                wildcardValues: ["any"],
              },
              {
                mode: "overlaps",
                requesterPath: ["interests"],
                candidatePath: ["interests"],
                emptyMatchesAll: true,
              },
            ],
          }),
          claimTtlMs: 2_000,
          claimId: "claim-mutual",
          matchId: "match-mutual",
          random: () => 0,
        });

        expect(result.status).toBe("matched");
      });
    },
  );

  integrationTest(
    "rolls back a claim and requeues both participants",
    async () => {
      await withStore(async (store) => {
        await add(store, participant("a"));
        await add(store, participant("b"));

        const result = await store.atomicClaim({
          requesterId: "a",
          poolKey: "chat",
          now: Date.now(),
          scanLimit: 50,
          excludeIds: [],
          strategy: { type: "fifo" },
          filter: filter(),
          claimTtlMs: 2_000,
          claimId: "claim-3",
          matchId: "match-3",
          idempotencyKey: "request-2",
          random: () => 0,
        });
        expect(result.status).toBe("matched");
        expect(await store.rollbackClaim("claim-3")).toBe(true);
        expect(await store.getQueueDepth("chat")).toBe(2);
        expect((await store.getParticipant("a"))?.state).toBe("waiting");
        expect((await store.getParticipant("b"))?.state).toBe("waiting");
      });
    },
  );

  integrationTest(
    "recovers an expired claim and requeues participants",
    async () => {
      await withStore(async (store) => {
        await add(store, participant("a"));
        await add(store, participant("b"));

        const result = await store.atomicClaim({
          requesterId: "a",
          poolKey: "chat",
          now: Date.now(),
          scanLimit: 50,
          excludeIds: [],
          strategy: { type: "fifo" },
          filter: filter(),
          claimTtlMs: 1_000,
          claimId: "claim-4",
          matchId: "match-4",
          idempotencyKey: "request-3",
          random: () => 0,
        });
        expect(result.status).toBe("matched");

        await new Promise((resolve) => setTimeout(resolve, 1_200));
        const recovered = await store.recoverExpiredClaims(Date.now());

        expect(recovered).toHaveLength(1);
        expect(recovered[0]?.state).toBe("expired");
        expect(await store.getQueueDepth("chat")).toBe(2);
        expect((await store.getParticipant("a"))?.state).toBe("waiting");
        expect((await store.getParticipant("b"))?.state).toBe("waiting");
      });
    },
  );

  integrationTest(
    "allows only one of two concurrent workers to claim a pair",
    async () => {
      await withStore(async (store, _redis, namespace) => {
        const secondWorker = new RedisCoordinationStore({
          redis: createRedis(),
          namespace,
          participantTtlSeconds: 120,
          claimTtlSeconds: 2,
          claimRetentionSeconds: 30,
        });
        await add(store, participant("a"));
        await add(store, participant("b"));
        await add(store, participant("c"));

        const results = await Promise.all([
          store.atomicClaim({
            requesterId: "a",
            poolKey: "chat",
            now: Date.now(),
            scanLimit: 50,
            excludeIds: [],
            strategy: { type: "fifo" },
            filter: filter(),
            claimTtlMs: 2_000,
            claimId: "claim-concurrent-1",
            matchId: "match-concurrent-1",
            random: () => 0,
          }),
          secondWorker.atomicClaim({
            requesterId: "c",
            poolKey: "chat",
            now: Date.now(),
            scanLimit: 50,
            excludeIds: [],
            strategy: { type: "fifo" },
            filter: filter(),
            claimTtlMs: 2_000,
            claimId: "claim-concurrent-2",
            matchId: "match-concurrent-2",
            random: () => 0,
          }),
        ]);

        expect(
          results.filter((result) => result.status === "matched"),
        ).toHaveLength(1);
        expect(
          results.filter((result) => result.status !== "matched"),
        ).toHaveLength(1);
      });
    },
  );

  integrationTest(
    "reports scan statistics and removes stale waiting entries",
    async () => {
      await withStore(async (store) => {
        const now = Date.now();
        await add(store, { ...participant("a"), joinedAt: now });
        await store.addToWaitingIndex({
          participantId: "c",
          poolKey: "chat",
          joinedAt: now + 1,
          version: 1,
        });
        await add(store, { ...participant("b"), joinedAt: now + 2 });

        const result = await store.atomicClaim({
          requesterId: "a",
          poolKey: "chat",
          now: Date.now(),
          scanLimit: 50,
          excludeIds: [],
          strategy: { type: "fifo" },
          filter: filter(),
          claimTtlMs: 2_000,
          claimId: "claim-1",
          matchId: "match-1",
          random: () => 0,
        });

        expect(result).toMatchObject({ status: "matched", scanned: 2, stale: 1 });
        expect(await store.getQueueDepth("chat")).toBe(0);
      });
    },
  );

  integrationTest(
    "starts random claims at an offset inside the scan window",
    async () => {
      await withStore(async (store) => {
        const now = Date.now();
        await add(store, { ...participant("a"), joinedAt: now });
        await add(store, { ...participant("b"), joinedAt: now + 1 });
        await add(store, { ...participant("c"), joinedAt: now + 2 });
        await add(store, { ...participant("d"), joinedAt: now + 3 });

        const result = await store.atomicClaim({
          requesterId: "a",
          poolKey: "chat",
          now: Date.now(),
          scanLimit: 50,
          excludeIds: [],
          strategy: { type: "random" },
          filter: filter(),
          claimTtlMs: 2_000,
          claimId: "claim-2",
          matchId: "match-2",
          random: () => 0.99,
        });

        expect(result).toMatchObject({ status: "matched", scanned: 1, stale: 0 });
        if (result.status === "matched") {
          expect(result.claim.strategy).toBe("random");
          expect(result.claim.participants.map(({ id }) => id)).toEqual([
            "a",
            "d",
          ]);
        }
      });
    },
  );
});
