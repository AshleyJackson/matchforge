import { FindMatchInputSchema, IdentifierSchema, ParticipantPreferencesSchema, PoolListSchema, RegisterParticipantSchema } from "./schemas";
import type {
  ClaimId, FindMatchOptions, IdempotencyKey, MatchClaim, MatchId, MatchRecord,
  MatcherOptions, Participant, ParticipantId, ParticipantPreferences, PoolKey, RegisterParticipantInput,
} from "./types";
import type { CoordinationStore, PersistenceStore } from "./types";
import { clonePreferences } from "./utils";

export class Matcher {
  private readonly store: CoordinationStore;
  private readonly persistence?: PersistenceStore;
  private readonly clock: () => number;
  private readonly idGenerator: () => string;
  private readonly random: () => number;
  private readonly defaultClaimTtlMs: number;
  private readonly defaultScanLimit: number;
  private readonly heartbeatTimeoutMs: number;

  constructor(options: MatcherOptions) {
    this.store = options.store;
    this.persistence = options.persistence;
    this.clock = options.clock ?? Date.now;
    this.idGenerator = options.idGenerator ?? (() => `${Date.now()}-${Math.random().toString(36).slice(2)}`);
    this.random = options.random ?? Math.random;
    this.defaultClaimTtlMs = options.defaultClaimTtlMs ?? 20_000;
    this.defaultScanLimit = options.defaultScanLimit ?? 100;
    this.heartbeatTimeoutMs = options.heartbeatTimeoutMs ?? 30_000;
  }

  async register(input: RegisterParticipantInput): Promise<Participant> {
    const validated = RegisterParticipantSchema.parse(input);
    const now = this.clock();
    const participant: Participant = {
      ...validated,
      pools: [...(validated.pools ?? [])],
      version: 1,
      state: "created",
      joinedAt: now,
      lastHeartbeatAt: now,
    };
    await this.store.putParticipant(participant);
    return participant;
  }

  async updatePreferences(id: ParticipantId, preferences: ParticipantPreferences, expectedVersion?: number): Promise<Participant> {
    IdentifierSchema.parse(id);
    const validatedPreferences = ParticipantPreferencesSchema.parse(preferences);
    const updated = await this.store.updateParticipant(id, (participant) => {
      if (expectedVersion !== undefined && participant.version !== expectedVersion) throw new Error("version conflict");
      return { ...participant, preferences: clonePreferences(validatedPreferences), version: participant.version + 1 };
    });
    if (!updated) throw new Error(`participant ${id} not found`);
    return updated;
  }

  async enqueue(id: ParticipantId, pools: PoolKey[], _options?: { idempotencyKey?: IdempotencyKey }): Promise<void> {
    IdentifierSchema.parse(id);
    const validatedPools = PoolListSchema.parse(pools);
    const now = this.clock();
    const participant = await this.store.updateParticipant(id, (current) => ({
      ...current,
      state: "waiting",
      pools: [...validatedPools],
      joinedAt: current.state === "waiting" ? current.joinedAt : now,
      lastHeartbeatAt: now,
      version: current.version + 1,
    }));
    if (!participant) throw new Error(`participant ${id} not found`);
    for (const poolKey of validatedPools) {
      await this.store.addToWaitingIndex({ participantId: id, poolKey, joinedAt: participant.joinedAt, version: participant.version });
    }
  }

  async dequeue(id: ParticipantId): Promise<void> {
    const participant = await this.store.updateParticipant(id, (current) => ({ ...current, state: "left", version: current.version + 1 }));
    if (!participant) return;
    for (const poolKey of participant.pools) await this.store.removeFromWaitingIndex(poolKey, id);
  }

  async heartbeat(id: ParticipantId): Promise<void> {
    const participant = await this.store.updateParticipant(id, (current) => ({ ...current, lastHeartbeatAt: this.clock() }));
    if (!participant) throw new Error(`participant ${id} not found`);
  }

  async findMatch(options: FindMatchOptions) {
    FindMatchInputSchema.parse(options);
    const result = await this.store.atomicClaim({
      ...options,
      now: this.clock(),
      scanLimit: options.scanLimit ?? this.defaultScanLimit,
      excludeIds: options.excludeIds ?? [],
      strategy: options.strategy ?? { type: "fifo" },
      filter: options.filter,
      claimTtlMs: options.claimTtlMs ?? this.defaultClaimTtlMs,
      claimId: this.idGenerator(),
      matchId: this.idGenerator(),
      random: this.random,
    });
    if (result.status !== "matched") return result;
    if (result.claim.state === "committed") return result;
    if (this.persistence) {
      try {
        await this.persistence.saveMatch({
          matchId: result.claim.matchId,
          claimId: result.claim.claimId,
          poolKey: result.claim.poolKey,
          participantIds: result.claim.participants.map((participant) => participant.id),
          createdAt: result.claim.createdAt,
          status: "active",
        });
        await this.commitClaim(result.claim.claimId);
      } catch (error) {
        await this.rollbackClaim(result.claim.claimId, "persistence failed");
        if (options.idempotencyKey) await this.store.clearIdempotency(options.idempotencyKey);
        return { status: "error" as const, error: error instanceof Error ? error : new Error(String(error)) };
      }
    }
    return result;
  }

  async commitClaim(claimId: ClaimId): Promise<void> {
    if (!(await this.store.commitClaim(claimId))) throw new Error(`claim ${claimId} is not reservable`);
  }

  async rollbackClaim(claimId: ClaimId, _reason?: string): Promise<void> {
    await this.store.rollbackClaim(claimId);
  }

  async completeMatch(matchId: MatchId): Promise<void> {
    if (this.persistence) await this.persistence.completeMatch(matchId);
  }

  getParticipant(id: ParticipantId): Promise<Participant | null> { return this.store.getParticipant(id); }
  getClaim(id: ClaimId): Promise<MatchClaim | null> { return this.store.getClaim(id); }
  getMatch(id: MatchId): Promise<MatchRecord | null> { return this.persistence?.getMatch(id) ?? Promise.resolve(null); }
  getQueueDepth(poolKey: PoolKey): Promise<number> { return this.store.getQueueDepth(poolKey); }
  recover(): Promise<MatchClaim[]> {
    return this.store.recoverExpiredClaims(this.clock(), async (claim) =>
      !!(await this.persistence?.getMatch(claim.matchId)),
    );
  }

  async isAlive(id: ParticipantId): Promise<boolean> {
    const participant = await this.getParticipant(id);
    return !!participant && this.clock() - participant.lastHeartbeatAt <= this.heartbeatTimeoutMs;
  }
}
