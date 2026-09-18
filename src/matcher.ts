import { FindMatchInputSchema, IdentifierSchema, ParticipantPreferencesSchema, PoolListSchema, RegisterParticipantSchema } from "./schemas.ts";
import type {
  ClaimId, FindMatchOptions, IdempotencyKey, MatchClaim, MatchId, MatchRecord,
  MatcherOptions, Participant, ParticipantId, ParticipantPreferences, PoolKey, RegisterParticipantInput,
} from "./types.ts";
import type { CoordinationStore, PersistenceStore } from "./types.ts";
import { cloneClaim, clonePreferences } from "./utils.ts";

export class Matcher {
  private readonly store: CoordinationStore;
  private readonly persistence?: PersistenceStore;
  private readonly clock: () => number;
  private readonly idGenerator: () => string;
  private readonly random: () => number;
  private readonly defaultClaimTtlMs: number;
  private readonly defaultScanLimit: number;
  private readonly heartbeatTimeoutMs: number;
  private readonly idempotency = new Map<IdempotencyKey, MatchClaim>();

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
    if (options.idempotencyKey) {
      const previous = this.idempotency.get(options.idempotencyKey);
      if (previous) return { status: "matched" as const, claim: cloneClaim(previous)! };
    }
    const result = await this.store.atomicClaim({
      ...options,
      now: this.clock(),
      scanLimit: options.scanLimit ?? this.defaultScanLimit,
      excludeIds: options.excludeIds ?? [],
      strategy: options.strategy ?? { type: "fifo" },
      claimTtlMs: options.claimTtlMs ?? this.defaultClaimTtlMs,
      claimId: this.idGenerator(),
      matchId: this.idGenerator(),
      random: this.random,
    });
    if (result.status !== "matched") return result;
    if (options.idempotencyKey) this.idempotency.set(options.idempotencyKey, cloneClaim(result.claim)!);
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
        return { status: "error" as const, error: error instanceof Error ? error : new Error(String(error)) };
      }
    }
    return result;
  }

  async commitClaim(claimId: ClaimId): Promise<void> {
    const claim = await this.store.getClaim(claimId);
    if (!claim || claim.state !== "reserved") throw new Error(`claim ${claimId} is not reservable`);
    claim.state = "committed";
    await this.store.putClaim(claim);
    for (const participant of claim.participants) {
      await this.store.updateParticipant(participant.id, (current) => ({ ...current, state: "matched", version: current.version + 1 }));
    }
  }

  async rollbackClaim(claimId: ClaimId, _reason?: string): Promise<void> {
    const claim = await this.store.getClaim(claimId);
    if (!claim || claim.state !== "reserved") return;
    claim.state = "rolled_back";
    await this.store.putClaim(claim);
    for (const participant of claim.participants) {
      const current = await this.store.updateParticipant(participant.id, (value) => ({ ...value, state: "waiting", version: value.version + 1 }));
      if (current) await this.store.addToWaitingIndex({ participantId: current.id, poolKey: claim.poolKey, joinedAt: current.joinedAt, version: current.version });
    }
  }

  async completeMatch(matchId: MatchId): Promise<void> {
    if (this.persistence) await this.persistence.completeMatch(matchId);
  }

  getParticipant(id: ParticipantId): Promise<Participant | null> { return this.store.getParticipant(id); }
  getClaim(id: ClaimId): Promise<MatchClaim | null> { return this.store.getClaim(id); }
  getMatch(id: MatchId): Promise<MatchRecord | null> { return this.persistence?.getMatch(id) ?? Promise.resolve(null); }
  getQueueDepth(poolKey: PoolKey): Promise<number> { return this.store.getQueueDepth(poolKey); }
  recover(): Promise<MatchClaim[]> { return this.store.recoverExpiredClaims(this.clock()); }

  async isAlive(id: ParticipantId): Promise<boolean> {
    const participant = await this.getParticipant(id);
    return !!participant && this.clock() - participant.lastHeartbeatAt <= this.heartbeatTimeoutMs;
  }
}
