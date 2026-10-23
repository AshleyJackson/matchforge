import {
  CoordinationStoreSchema,
  FindMatchInputSchemaWithFunctions,
  IdentifierSchema,
  MatchClaimSchema,
  MatchRecordSchema,
  MatcherOptionsSchema,
  ParticipantPreferencesSchema,
  ParticipantSchema,
  PoolListSchema,
  RegisterParticipantInputSchema,
  PersistenceStoreSchema,
  SyncParticipantInputSchema,
  SyncParticipantResultSchema,
} from "./schemas";
import { z } from "zod";
import { clonePreferences, stableSerialize } from "./utils";

export class Matcher {
  private readonly store: z.infer<typeof CoordinationStoreSchema>;
  private readonly persistence?: z.infer<typeof PersistenceStoreSchema>;
  private readonly clock: () => number;
  private readonly idGenerator: () => string;
  private readonly random: () => number;
  private readonly defaultClaimTtlMs: number;
  private readonly defaultScanLimit: number;
  private readonly heartbeatTimeoutMs: number;

  constructor(options: z.infer<typeof MatcherOptionsSchema>) {
    this.store = options.store;
    this.persistence = options.persistence;
    this.clock = options.clock ?? Date.now;
    this.idGenerator =
      options.idGenerator ??
      (() => `${Date.now()}-${Math.random().toString(36).slice(2)}`);
    this.random = options.random ?? Math.random;
    this.defaultClaimTtlMs = options.defaultClaimTtlMs ?? 20_000;
    this.defaultScanLimit = options.defaultScanLimit ?? 100;
    this.heartbeatTimeoutMs = options.heartbeatTimeoutMs ?? 30_000;
  }

  async register(input: z.infer<typeof RegisterParticipantInputSchema>): Promise<z.infer<typeof ParticipantSchema>> {
    const validated = RegisterParticipantInputSchema.parse(input);
    const now = this.clock();
    const participant: z.infer<typeof ParticipantSchema> = {
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

  async updatePreferences(
    id: string,
    preferences: z.infer<typeof ParticipantPreferencesSchema>,
    expectedVersion?: number,
  ): Promise<z.infer<typeof ParticipantSchema>> {
    IdentifierSchema.parse(id);
    const validatedPreferences =
      ParticipantPreferencesSchema.parse(preferences);
    const updated = await this.store.updateParticipant(
      id,
      (participant: z.infer<typeof ParticipantSchema>) => {
      if (
        expectedVersion !== undefined &&
        participant.version !== expectedVersion
      )
        throw new Error("version conflict");
      return {
        ...participant,
        preferences: clonePreferences(validatedPreferences),
        version: participant.version + 1,
      };
      },
    );
    if (!updated) throw new Error(`participant ${id} not found`);
    return updated;
  }

  async enqueue(
    id: string,
    pools: string[],
    _options?: { idempotencyKey?: string },
  ): Promise<void> {
    IdentifierSchema.parse(id);
    const validatedPools = PoolListSchema.parse(pools);
    const now = this.clock();
    const participant = await this.store.updateParticipant(id, (current: z.infer<typeof ParticipantSchema>) => ({
      ...current,
      state: "waiting",
      pools: [...validatedPools],
      joinedAt: current.state === "waiting" ? current.joinedAt : now,
      lastHeartbeatAt: now,
      version: current.version + 1,
    }));
    if (!participant) throw new Error(`participant ${id} not found`);
    for (const poolKey of validatedPools) {
      await this.store.addToWaitingIndex({
        participantId: id,
        poolKey,
        joinedAt: participant.joinedAt,
        version: participant.version,
      });
    }
  }

  async dequeue(id: string): Promise<void> {
    const participant = await this.store.updateParticipant(id, (current: z.infer<typeof ParticipantSchema>) => ({
      ...current,
      state: "left",
      version: current.version + 1,
    }));
    if (!participant) return;
    for (const poolKey of participant.pools)
      await this.store.removeFromWaitingIndex(poolKey, id);
  }

  /**
   * Writes a participant's desired coordination state and waiting-index
   * entries in one atomic step, creating the record when it is missing.
   * Expired exclusions are dropped before writing. See
   * `SyncParticipantResultSchema` for the outcomes; only `created` and
   * `updated` write anything.
   */
  async syncParticipant(
    input: z.infer<typeof SyncParticipantInputSchema>,
  ): Promise<z.infer<typeof SyncParticipantResultSchema>> {
    const validated = SyncParticipantInputSchema.parse(input);
    if (!this.store.syncParticipant)
      throw new Error("coordination store does not support syncParticipant");
    const now = this.clock();
    const exclusions = validated.exclusions
      ? Object.fromEntries(
          Object.entries(validated.exclusions).filter(([, until]) => until > now),
        )
      : undefined;
    return this.store.syncParticipant({
      ...validated,
      ...(exclusions ? { exclusions } : {}),
    });
  }

  async heartbeat(id: string): Promise<void> {
    const participant = await this.store.updateParticipant(id, (current: z.infer<typeof ParticipantSchema>) => ({
      ...current,
      lastHeartbeatAt: this.clock(),
    }));
    if (!participant) throw new Error(`participant ${id} not found`);
  }

  async findMatch(options: z.infer<typeof FindMatchInputSchemaWithFunctions>) {
    FindMatchInputSchemaWithFunctions.parse(options);
    const result = await this.store.atomicClaim({
      ...options,
      now: this.clock(),
      scanLimit: options.scanLimit ?? this.defaultScanLimit,
      excludeIds: options.excludeIds ?? [],
      maxHeartbeatAgeMs: options.maxHeartbeatAgeMs,
      cursor: options.cursor,
      strategy: options.strategy ?? { type: "fifo" },
      filter: options.filter,
      claimTtlMs: options.claimTtlMs ?? this.defaultClaimTtlMs,
      claimId: this.idGenerator(),
      matchId: this.idGenerator(),
      requestFingerprint: stableSerialize({
        requesterId: options.requesterId,
        poolKey: options.poolKey,
        strategy: options.strategy ?? { type: "fifo" },
        scanLimit: options.scanLimit ?? this.defaultScanLimit,
        excludeIds: options.excludeIds ?? [],
        filter: options.filter ?? null,
        ...(options.maxHeartbeatAgeMs !== undefined
          ? { maxHeartbeatAgeMs: options.maxHeartbeatAgeMs }
          : {}),
        hasCompatibility: Boolean(options.compatibility),
        hasScore: Boolean(options.score),
        hasRelaxation: Boolean(options.relax),
      }),
      random: this.random,
    });
    if (result.status !== "matched") return result;
    // A replayed `reserved` claim belongs to an attempt that never finished
    // (or is still running). Saving by `matchId` is idempotent and commits
    // are idempotent, so completing it here is safe either way.
    if (result.claim.state === "committed") return result;
    if (this.persistence) {
      try {
        await this.persistence.saveMatch({
          matchId: result.claim.matchId,
          claimId: result.claim.claimId,
          poolKey: result.claim.poolKey,
          participantIds: result.claim.participants.map(
            (participant) => participant.id,
          ),
          createdAt: result.claim.createdAt,
          status: "active",
        });
        await this.commitClaim(result.claim.claimId);
      } catch (error) {
        await this.rollbackClaim(result.claim.claimId, "persistence failed");
        if (options.idempotencyKey)
          await this.store.clearIdempotency(options.idempotencyKey);
        return {
          status: "error" as const,
          error: error instanceof Error ? error : new Error(String(error)),
        };
      }
    } else {
      await this.commitClaim(result.claim.claimId);
    }
    const committed = await this.store.getClaim(result.claim.claimId);
    return committed ? { ...result, claim: committed } : result;
  }

  async commitClaim(claimId: string): Promise<void> {
    if (!(await this.store.commitClaim(claimId)))
      throw new Error(`claim ${claimId} is not reservable`);
  }

  async rollbackClaim(claimId: string, _reason?: string): Promise<void> {
    await this.store.rollbackClaim(claimId);
  }

  async completeMatch(matchId: string): Promise<void> {
    if (this.persistence) await this.persistence.completeMatch(matchId);
  }

  getParticipant(id: string): Promise<z.infer<typeof ParticipantSchema> | null> {
    return this.store.getParticipant(id);
  }
  getClaim(id: string): Promise<z.infer<typeof MatchClaimSchema> | null> {
    return this.store.getClaim(id);
  }
  getMatch(id: string): Promise<z.infer<typeof MatchRecordSchema> | null> {
    return this.persistence?.getMatch(id) ?? Promise.resolve(null);
  }
  getQueueDepth(poolKey: string): Promise<number> {
    return this.store.getQueueDepth(poolKey);
  }
  /** Recovers at most `limit` (default 100) expired claims per call. */
  recover(options: { limit?: number } = {}): Promise<z.infer<typeof MatchClaimSchema>[]> {
    return this.store.recoverExpiredClaims(
      this.clock(),
      async (claim: z.infer<typeof MatchClaimSchema>) =>
        !!(await this.persistence?.getMatch(claim.matchId)),
      options,
    );
  }

  async isAlive(id: string): Promise<boolean> {
    const participant = await this.getParticipant(id);
    return (
      !!participant &&
      this.clock() - participant.lastHeartbeatAt <= this.heartbeatTimeoutMs
    );
  }
}
