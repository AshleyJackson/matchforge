import { describe, expect, test } from "bun:test";
import { z } from "zod";
import {
  MatchClaimSchema,
  ParticipantSchema,
  RedisCoordinationStore,
  buildMutualCompatibilityFilter,
} from "../index.ts";

class FakeRedis {
  readonly evaluations: Array<{
    script: string;
    keys: string[];
    args: string[];
  }> = [];
  private readonly values = new Map<string, unknown>();
  private readonly sets = new Map<string, Set<string>>();

  async get<T>(key: string): Promise<T | null> {
    return (this.values.get(key) as T | undefined) ?? null;
  }
  async set(key: string, value: unknown): Promise<string> {
    this.values.set(key, value);
    return "OK";
  }
  async del(...keys: string[]): Promise<number> {
    return keys.reduce(
      (count, key) => count + (this.values.delete(key) ? 1 : 0),
      0,
    );
  }
  async zadd(): Promise<number> {
    return 1;
  }
  async zrem(): Promise<number> {
    return 1;
  }
  async zscore(): Promise<number | null> {
    return 1;
  }
  async zrange<T extends string[]>(): Promise<T> {
    return [] as unknown as T;
  }
  async sadd(key: string, ...members: string[]): Promise<number> {
    const set = this.sets.get(key) ?? new Set<string>();
    members.forEach((member) => set.add(member));
    this.sets.set(key, set);
    return members.length;
  }
  async srem(): Promise<number> {
    return 1;
  }
  async smembers<T extends string[]>(): Promise<T> {
    return [] as unknown as T;
  }
  async eval<T = unknown>(
    script: string,
    keys: string[],
    args: string[],
  ): Promise<T> {
    this.evaluations.push({ script, keys, args });
    if (script.includes("claim.state = 'committed'")) return ["committed"] as T;
    if (script.includes("claim.state = 'rolled_back'"))
      return ["rolled_back"] as T;
    return ["matched", JSON.stringify(claim)] as T;
  }
}

const participant = (id: string): z.infer<typeof ParticipantSchema> => ({
  id,
  preferences: { interests: ["games"] },
  version: 1,
  state: "waiting",
  joinedAt: 1_000,
  lastHeartbeatAt: 1_000,
  pools: ["chat"],
});

const claim: z.infer<typeof MatchClaimSchema> = {
  claimId: "claim-1",
  matchId: "match-1",
  poolKey: "chat",
  participants: [
    { id: "a", version: 1, snapshot: { interests: ["games"] } },
    { id: "b", version: 1, snapshot: { interests: ["games"] } },
  ],
  state: "reserved",
  createdAt: 1_000,
  expiresAt: 21_000,
  strategy: "fifo",
  recoveryToken: "claim-1",
  requestFingerprint: "test-request",
};

describe("Redis coordination contract", () => {
  test("requires a serializable filter and sends its AST to the adapter", async () => {
    const redis = new FakeRedis();
    const store = new RedisCoordinationStore({ redis });
    await store.putParticipant(participant("a"));
    await store.putParticipant(participant("b"));

    const result = await store.atomicClaim({
      requesterId: "a",
      poolKey: "chat",
      now: 1_000,
      scanLimit: 50,
      excludeIds: [],
      strategy: { type: "fifo" },
      filter: buildMutualCompatibilityFilter({
        rules: [
          {
            mode: "overlaps",
            requesterPath: ["interests"],
            candidatePath: ["interests"],
            emptyMatchesAll: true,
          },
        ],
      }),
      claimTtlMs: 20_000,
      claimId: "claim-1",
      matchId: "match-1",
      random: () => 0,
    });

    expect(result.status).toBe("matched");
    expect(redis.evaluations[0]?.args[12]).toContain('"op":"overlaps"');
    expect(
      redis.evaluations[0]?.keys.every((key) => key.includes("{matchforge:v1}")),
    ).toBe(true);
    expect(redis.evaluations[0]?.script).toContain("is-empty");
    expect(
      (
        await store.atomicClaim({
          requesterId: "a",
          poolKey: "chat",
          now: 1_000,
          scanLimit: 50,
          excludeIds: [],
          strategy: { type: "fifo" },
          claimTtlMs: 20_000,
          claimId: "claim-2",
          matchId: "match-2",
          random: () => 0,
        })
      ).status,
    ).toBe("error");
  });

  test("uses adapter-side atomic lifecycle scripts", async () => {
    const redis = new FakeRedis();
    const store = new RedisCoordinationStore({ redis });
    await store.putClaim(claim);
    expect(await store.commitClaim("claim-1")).toBe(true);
    expect(await store.rollbackClaim("claim-1")).toBe(true);
    expect(
      redis.evaluations.some(({ script }) =>
        script.includes("claim.state = 'committed'"),
      ),
    ).toBe(true);
    expect(
      redis.evaluations.some(({ script }) =>
        script.includes("claim.state = 'rolled_back'"),
      ),
    ).toBe(true);
  });
});
