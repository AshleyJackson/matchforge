import { z } from "zod";
import { applyRelaxation, selectCandidate } from "./selection";
import { evaluateSerializableFilter } from "./filters";
import {
  MatchClaimSchema,
  ParticipantSchema,
  WaitingEntrySchema,
  AtomicClaimInputSchema,
  ClaimResultSchema,
  SyncParticipantInputSchema,
  SyncParticipantResultSchema,
} from "./schemas";
import {
  claimRequestFingerprint,
  cloneClaim,
  cloneParticipant,
  clonePreferences,
  decodeClaimCursor,
  encodeClaimCursor,
  isExcluded,
  syncedParticipantBody,
} from "./utils";

class SerialExecutor {
  private tail = Promise.resolve();

  run<T>(operation: () => Promise<T> | T): Promise<T> {
    const result = this.tail.then(operation, operation);
    this.tail = result.then(
      () => undefined,
      () => undefined,
    );
    return result;
  }
}

type WaitingEntry = z.infer<typeof WaitingEntrySchema>;

function compareEntries(a: WaitingEntry, b: WaitingEntry): number {
  if (a.joinedAt !== b.joinedAt) return a.joinedAt - b.joinedAt;
  if (a.participantId === b.participantId) return 0;
  return a.participantId < b.participantId ? -1 : 1;
}

export class InMemoryCoordinationStore {
  private readonly participants = new Map<string, z.infer<typeof ParticipantSchema>>();
  private readonly waiting = new Map<string, Map<string, WaitingEntry>>();
  private readonly claims = new Map<string, z.infer<typeof MatchClaimSchema>>();
  /** Idempotency key -> the claim it produced. Replays read the live claim. */
  private readonly idempotency = new Map<
    string,
    { claimId: string; requestFingerprint: string }
  >();
  private readonly executor = new SerialExecutor();

  async putParticipant(participant: z.infer<typeof ParticipantSchema>): Promise<void> {
    const validated = ParticipantSchema.parse(participant);
    await this.executor.run(() =>
      this.participants.set(validated.id, cloneParticipant(validated)!),
    );
  }

  async getParticipant(id: string): Promise<z.infer<typeof ParticipantSchema> | null> {
    return this.executor.run(async () => {
      const participant = this.participants.get(id);
      return participant
        ? cloneParticipant(ParticipantSchema.parse(participant))
        : null;
    });
  }

  async updateParticipant(
    id: string,
    update: (participant: z.infer<typeof ParticipantSchema>) => z.infer<typeof ParticipantSchema>,
  ): Promise<z.infer<typeof ParticipantSchema> | null> {
    return this.executor.run(async () => {
      const current = this.participants.get(id);
      if (!current) return null;
      const next = update(cloneParticipant(current)!);
      this.participants.set(id, cloneParticipant(next)!);
      return cloneParticipant(next);
    });
  }

  async syncParticipant(
    input: z.infer<typeof SyncParticipantInputSchema>,
  ): Promise<z.infer<typeof SyncParticipantResultSchema>> {
    const validated = SyncParticipantInputSchema.parse(input);
    return this.executor.run(() => {
      const current = this.participants.get(validated.id) ?? null;
      if (current?.state === "claimed")
        return { status: "claimed", participant: cloneParticipant(current)! };
      if (
        validated.expectedVersion !== undefined &&
        (current?.version ?? 0) !== validated.expectedVersion
      )
        return { status: "conflict", participant: cloneParticipant(current) };
      if (
        validated.revision !== undefined &&
        current?.syncRevision !== undefined &&
        current.syncRevision >= validated.revision
      )
        return { status: "stale", participant: cloneParticipant(current)! };

      const next = ParticipantSchema.parse({
        ...(current?.syncRevision !== undefined
          ? { syncRevision: current.syncRevision }
          : {}),
        ...syncedParticipantBody(validated),
        version: (current?.version ?? 0) + 1,
      });
      this.participants.set(next.id, next);
      const keep = new Set(next.state === "waiting" ? next.pools : []);
      for (const poolKey of new Set([...(current?.pools ?? []), ...next.pools])) {
        if (!keep.has(poolKey)) this.waiting.get(poolKey)?.delete(next.id);
      }
      for (const poolKey of keep) this.addEntryUnsafe(next, poolKey);
      return {
        status: current ? "updated" : "created",
        participant: cloneParticipant(next)!,
      };
    });
  }

  async addToWaitingIndex(entry: WaitingEntry): Promise<void> {
    const validated = WaitingEntrySchema.parse(entry);
    await this.executor.run(() => {
      let pool = this.waiting.get(validated.poolKey);
      if (!pool) {
        pool = new Map();
        this.waiting.set(validated.poolKey, pool);
      }
      pool.set(validated.participantId, { ...validated });
    });
  }

  async removeFromWaitingIndex(
    poolKey: string,
    participantId: string,
  ): Promise<void> {
    await this.executor.run(() =>
      this.waiting.get(poolKey)?.delete(participantId),
    );
  }

  async getClaim(claimId: string): Promise<z.infer<typeof MatchClaimSchema> | null> {
    return this.executor.run(() => {
      const claim = this.claims.get(claimId);
      return claim ? cloneClaim(MatchClaimSchema.parse(claim)) : null;
    });
  }

  async putClaim(claim: z.infer<typeof MatchClaimSchema>): Promise<void> {
    const validated = MatchClaimSchema.parse(claim);
    await this.executor.run(() =>
      this.claims.set(validated.claimId, cloneClaim(validated)!),
    );
  }

  async commitClaim(claimId: string): Promise<boolean> {
    return this.executor.run(() => {
      const claim = this.claims.get(claimId);
      if (claim?.state === "committed") return true;
      if (!claim || claim.state !== "reserved") return false;
      const currentParticipants = claim.participants.map((participant) =>
        this.participants.get(participant.id),
      );
      if (
        currentParticipants.some(
          (current, index) =>
            !current ||
            current.state !== "claimed" ||
            current.version !== claim.participants[index]!.version + 1,
        )
      )
        return false;
      claim.state = "committed";
      for (const [index, participant] of claim.participants.entries()) {
        const current = currentParticipants[index]!;
        this.participants.set(participant.id, {
          ...current,
          state: "matched",
          version: current.version + 1,
        });
      }
      return true;
    });
  }

  async rollbackClaim(claimId: string): Promise<boolean> {
    return this.executor.run(() => {
      const claim = this.claims.get(claimId);
      if (!claim || claim.state !== "reserved") return false;
      claim.state = "rolled_back";
      this.requeueUnsafe(claim);
      this.releaseIdempotencyUnsafe(claim);
      return true;
    });
  }

  async getQueueDepth(poolKey: string): Promise<number> {
    return this.executor.run(() => this.waiting.get(poolKey)?.size ?? 0);
  }

  async atomicClaim(input: z.infer<typeof AtomicClaimInputSchema>): Promise<z.infer<typeof ClaimResultSchema>> {
    return this.executor.run(async () => {
      const fingerprint =
        input.requestFingerprint ?? claimRequestFingerprint(input);
      if (input.idempotencyKey) {
        const previous = this.idempotency.get(input.idempotencyKey);
        const previousClaim = previous && this.claims.get(previous.claimId);
        if (
          previous &&
          previousClaim &&
          (previousClaim.state === "reserved" || previousClaim.state === "committed")
        ) {
          if (previous.requestFingerprint !== fingerprint) {
            return {
              status: "error",
              error: new Error("idempotency key reused with different request"),
            };
          }
          return { status: "matched", claim: cloneClaim(previousClaim)!, replayed: true };
        }
        this.idempotency.delete(input.idempotencyKey);
      }
      const requester = this.participants.get(input.requesterId);
      if (!requester || requester.state !== "waiting")
        return { status: "stale", reason: "requester is not waiting" };
      const pool = this.waiting.get(input.poolKey);
      if (!pool?.has(requester.id))
        return { status: "stale", reason: "requester is not queued" };
      const excluded = new Set([requester.id, ...input.excludeIds]);
      const after = input.cursor ? decodeClaimCursor(input.cursor) : null;
      const ordered = [...pool.values()]
        .filter(
          (entry) =>
            !after ||
            compareEntries(entry, {
              participantId: after.participantId,
              poolKey: entry.poolKey,
              joinedAt: after.joinedAt,
              version: 0,
            }) > 0,
        )
        .sort(compareEntries);
      const entries: WaitingEntry[] = [];
      let consumed = 0;
      for (const entry of ordered) {
        if (entries.length >= Math.max(0, input.scanLimit)) break;
        consumed += 1;
        if (!excluded.has(entry.participantId)) entries.push(entry);
      }
      const candidates = [];
      const requesterForRules = applyRelaxation(
        requester,
        input.relax,
        input.now,
      );

      for (let index = 0; index < entries.length; index += 1) {
        const entry = entries[index]!;
        const candidate = this.participants.get(entry.participantId);
        if (
          !candidate ||
          candidate.state !== "waiting" ||
          candidate.version !== entry.version
        )
          continue;
        if (
          input.maxHeartbeatAgeMs !== undefined &&
          input.now - candidate.lastHeartbeatAt > input.maxHeartbeatAgeMs
        )
          continue;
        if (
          isExcluded(requester, candidate.id, input.now) ||
          isExcluded(candidate, requester.id, input.now)
        )
          continue;
        const context = {
          now: input.now,
          waitTimeMs: Math.max(0, input.now - requester.joinedAt),
          poolKey: input.poolKey,
          attempt: index,
        };
        const candidateForRules = applyRelaxation(
          candidate,
          input.relax,
          input.now,
        );
        if (
          input.filter &&
          !evaluateSerializableFilter(input.filter, requester, candidate)
        )
          continue;
        if (
          input.compatibility &&
          !(await input.compatibility(
            requesterForRules,
            candidateForRules,
            context,
          ))
        )
          continue;
        const score = input.score
          ? await input.score(requesterForRules, candidateForRules, context)
          : 0;
        if (score === null || Number.isNaN(score)) continue;
        candidates.push({ participant: candidate, score, entry });
      }

      const selected = selectCandidate(
        candidates,
        input.strategy,
        input.random,
      );
      if (!selected) {
        const last = ordered[consumed - 1];
        return consumed < ordered.length && last
          ? {
              status: "none",
              cursor: encodeClaimCursor(last.joinedAt, last.participantId),
            }
          : { status: "none" };
      }
      const claim: z.infer<typeof MatchClaimSchema> = {
        claimId: input.claimId,
        matchId: input.matchId,
        poolKey: input.poolKey,
        participants: [requester, selected.participant].map((participant) => ({
          id: participant.id,
          version: participant.version,
          snapshot: clonePreferences(participant.preferences),
        })),
        state: "reserved",
        createdAt: input.now,
        expiresAt: input.now + input.claimTtlMs,
        strategy: input.strategy.type,
        recoveryToken: input.idempotencyKey ?? input.claimId,
        requestFingerprint: fingerprint,
      };
      pool.delete(requester.id);
      pool.delete(selected.participant.id);
      this.participants.set(requester.id, {
        ...requester,
        state: "claimed",
        version: requester.version + 1,
      });
      this.participants.set(selected.participant.id, {
        ...selected.participant,
        state: "claimed",
        version: selected.participant.version + 1,
      });
      this.claims.set(claim.claimId, claim);
      if (input.idempotencyKey)
        this.idempotency.set(input.idempotencyKey, {
          claimId: claim.claimId,
          requestFingerprint: fingerprint,
        });
      return { status: "matched", claim: cloneClaim(claim)! };
    });
  }

  async clearIdempotency(key: string): Promise<void> {
    await this.executor.run(() => this.idempotency.delete(key));
  }

  async recoverExpiredClaims(
    now: number,
    hasDurableMatch?: (claim: z.infer<typeof MatchClaimSchema>) => boolean | Promise<boolean>,
    options: { limit?: number } = {},
  ): Promise<z.infer<typeof MatchClaimSchema>[]> {
    return this.executor.run(async () => {
      const due = [...this.claims.values()]
        .filter((claim) => claim.state === "reserved" && claim.expiresAt <= now)
        .sort((a, b) => a.expiresAt - b.expiresAt)
        .slice(0, options.limit ?? 100);
      const expired: z.infer<typeof MatchClaimSchema>[] = [];
      for (const claim of due) {
        if (hasDurableMatch && (await hasDurableMatch(cloneClaim(claim)!))) {
          claim.state = "committed";
          for (const participant of claim.participants) {
            const current = this.participants.get(participant.id);
            if (
              current?.state === "claimed" &&
              current.version === participant.version + 1
            ) {
              this.participants.set(participant.id, {
                ...current,
                state: "matched",
                version: current.version + 1,
              });
            }
          }
          continue;
        }
        claim.state = "expired";
        this.requeueUnsafe(claim);
        this.releaseIdempotencyUnsafe(claim);
        expired.push(cloneClaim(claim)!);
      }
      return expired;
    });
  }

  private requeueUnsafe(claim: z.infer<typeof MatchClaimSchema>): void {
    for (const claimed of claim.participants) {
      const participant = this.participants.get(claimed.id);
      if (
        !participant ||
        participant.state !== "claimed" ||
        participant.version !== claimed.version + 1
      )
        continue;
      const next = {
        ...participant,
        state: "waiting" as const,
        version: participant.version + 1,
      };
      this.participants.set(participant.id, next);
      this.addEntryUnsafe(next, claim.poolKey);
    }
  }

  /** Frees the claim's idempotency key so a retry starts a new search. */
  private releaseIdempotencyUnsafe(claim: z.infer<typeof MatchClaimSchema>): void {
    if (this.idempotency.get(claim.recoveryToken)?.claimId === claim.claimId)
      this.idempotency.delete(claim.recoveryToken);
  }

  private addEntryUnsafe(
    participant: z.infer<typeof ParticipantSchema>,
    poolKey: string,
  ): void {
    let pool = this.waiting.get(poolKey);
    if (!pool) {
      pool = new Map();
      this.waiting.set(poolKey, pool);
    }
    pool.set(participant.id, {
      participantId: participant.id,
      poolKey,
      joinedAt: participant.joinedAt,
      version: participant.version,
    });
  }
}
