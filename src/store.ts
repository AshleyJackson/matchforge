import type {
  AtomicClaimInput, ClaimResult, CoordinationStore, MatchClaim, Participant,
  ParticipantId, PoolKey, WaitingEntry,
} from "./types";
import { applyRelaxation, selectCandidate } from "./selection";
import { evaluateSerializableFilter } from "./filters";
import { MatchClaimSchema, ParticipantSchema, WaitingEntrySchema } from "./schemas";
import { cloneClaim, cloneParticipant, clonePreferences } from "./utils";

class SerialExecutor {
  private tail = Promise.resolve();

  run<T>(operation: () => Promise<T> | T): Promise<T> {
    const result = this.tail.then(operation, operation);
    this.tail = result.then(() => undefined, () => undefined);
    return result;
  }
}

export class InMemoryCoordinationStore implements CoordinationStore {
  private readonly participants = new Map<ParticipantId, Participant>();
  private readonly waiting = new Map<PoolKey, Map<ParticipantId, WaitingEntry>>();
  private readonly claims = new Map<string, MatchClaim>();
  private readonly idempotency = new Map<string, MatchClaim>();
  private readonly executor = new SerialExecutor();

  async putParticipant(participant: Participant): Promise<void> {
    const validated = ParticipantSchema.parse(participant);
    await this.executor.run(() => this.participants.set(validated.id, cloneParticipant(validated)!));
  }

  async getParticipant(id: ParticipantId): Promise<Participant | null> {
    return this.executor.run(async () => {
      const participant = this.participants.get(id);
      return participant ? cloneParticipant(ParticipantSchema.parse(participant)) : null;
    });
  }

  async updateParticipant(id: ParticipantId, update: (participant: Participant) => Participant): Promise<Participant | null> {
    return this.executor.run(async () => {
      const current = this.participants.get(id);
      if (!current) return null;
      const next = update(cloneParticipant(current)!);
      this.participants.set(id, cloneParticipant(next)!);
      return cloneParticipant(next);
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

  async removeFromWaitingIndex(poolKey: PoolKey, participantId: ParticipantId): Promise<void> {
    await this.executor.run(() => this.waiting.get(poolKey)?.delete(participantId));
  }

  async getClaim(claimId: string): Promise<MatchClaim | null> {
    return this.executor.run(() => {
      const claim = this.claims.get(claimId);
      return claim ? cloneClaim(MatchClaimSchema.parse(claim)) : null;
    });
  }

  async putClaim(claim: MatchClaim): Promise<void> {
    const validated = MatchClaimSchema.parse(claim);
    await this.executor.run(() => this.claims.set(validated.claimId, cloneClaim(validated)!));
  }

  async commitClaim(claimId: string): Promise<boolean> {
    return this.executor.run(() => {
      const claim = this.claims.get(claimId);
      if (!claim || claim.state !== "reserved") return false;
      claim.state = "committed";
      for (const participant of claim.participants) {
        const current = this.participants.get(participant.id);
        if (current?.state === "claimed" && current.version === participant.version + 1) {
          this.participants.set(participant.id, { ...current, state: "matched", version: current.version + 1 });
        }
      }
      for (const [key, idempotentClaim] of this.idempotency) {
        if (idempotentClaim.claimId === claimId) {
          this.idempotency.set(key, cloneClaim({ ...claim })!);
        }
      }
      return true;
    });
  }

  async rollbackClaim(claimId: string): Promise<boolean> {
    return this.executor.run(() => {
      const claim = this.claims.get(claimId);
      if (!claim || claim.state !== "reserved") return false;
      claim.state = "rolled_back";
      for (const participant of claim.participants) {
        const current = this.participants.get(participant.id);
        if (!current || current.state !== "claimed" || current.version !== participant.version + 1) continue;
        const next = { ...current, state: "waiting" as const, version: current.version + 1 };
        this.participants.set(participant.id, next);
        this.addEntryUnsafe(next, claim.poolKey);
      }
      return true;
    });
  }

  async getQueueDepth(poolKey: PoolKey): Promise<number> {
    return this.executor.run(() => this.waiting.get(poolKey)?.size ?? 0);
  }

  async atomicClaim(input: AtomicClaimInput): Promise<ClaimResult> {
    return this.executor.run(async () => {
      if (input.idempotencyKey) {
        const previous = this.idempotency.get(input.idempotencyKey);
        if (previous) return { status: "matched", claim: cloneClaim(previous)! };
      }
      const requester = this.participants.get(input.requesterId);
      if (!requester || requester.state !== "waiting") return { status: "stale", reason: "requester is not waiting" };
      const pool = this.waiting.get(input.poolKey);
      if (!pool?.has(requester.id)) return { status: "stale", reason: "requester is not queued" };
      const excluded = new Set([requester.id, ...input.excludeIds]);
      const entries = [...pool.values()]
        .filter((entry) => !excluded.has(entry.participantId))
        .sort((a, b) => a.joinedAt - b.joinedAt)
        .slice(0, Math.max(0, input.scanLimit));
      const candidates = [];
      const requesterForRules = applyRelaxation(requester, input.relax, input.now);

      for (let index = 0; index < entries.length; index += 1) {
        const entry = entries[index]!;
        const candidate = this.participants.get(entry.participantId);
        if (!candidate || candidate.state !== "waiting" || candidate.version !== entry.version) continue;
        const context = {
          now: input.now,
          waitTimeMs: Math.max(0, input.now - requester.joinedAt),
          poolKey: input.poolKey,
          attempt: index,
        };
        const candidateForRules = applyRelaxation(candidate, input.relax, input.now);
        if (input.filter && !evaluateSerializableFilter(input.filter, requester, candidate)) continue;
        if (input.compatibility && !(await input.compatibility(requesterForRules, candidateForRules, context))) continue;
        const score = input.score ? await input.score(requesterForRules, candidateForRules, context) : 0;
        if (score === null || Number.isNaN(score)) continue;
        candidates.push({ participant: candidate, score, entry });
      }

      const selected = selectCandidate(candidates, input.strategy, input.random);
      if (!selected) return { status: "none" };
      const claim: MatchClaim = {
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
      };
      pool.delete(requester.id);
      pool.delete(selected.participant.id);
      this.participants.set(requester.id, { ...requester, state: "claimed", version: requester.version + 1 });
      this.participants.set(selected.participant.id, { ...selected.participant, state: "claimed", version: selected.participant.version + 1 });
      this.claims.set(claim.claimId, claim);
      if (input.idempotencyKey) this.idempotency.set(input.idempotencyKey, cloneClaim(claim)!);
      return { status: "matched", claim: cloneClaim(claim)! };
    });
  }

  async clearIdempotency(key: string): Promise<void> {
    await this.executor.run(() => this.idempotency.delete(key));
  }

  async recoverExpiredClaims(
    now: number,
    hasDurableMatch?: (claim: MatchClaim) => Promise<boolean>,
  ): Promise<MatchClaim[]> {
    return this.executor.run(async () => {
      const expired: MatchClaim[] = [];
      for (const claim of this.claims.values()) {
        if (claim.state !== "reserved" || claim.expiresAt > now) continue;
        if (hasDurableMatch && await hasDurableMatch(cloneClaim(claim)!)) {
          claim.state = "committed";
          for (const participant of claim.participants) {
            const current = this.participants.get(participant.id);
            if (current?.state === "claimed" && current.version === participant.version + 1) {
              this.participants.set(participant.id, { ...current, state: "matched", version: current.version + 1 });
            }
          }
          continue;
        }
        claim.state = "expired";
        for (const claimed of claim.participants) {
          const participant = this.participants.get(claimed.id);
          if (!participant || participant.state !== "claimed" || participant.version !== claimed.version + 1) continue;
          const next = { ...participant, state: "waiting" as const, version: participant.version + 1 };
          this.participants.set(participant.id, next);
          this.addEntryUnsafe(next, claim.poolKey);
        }
        expired.push(cloneClaim(claim)!);
      }
      return expired;
    });
  }

  private addEntryUnsafe(participant: Participant, poolKey: PoolKey): void {
    let pool = this.waiting.get(poolKey);
    if (!pool) {
      pool = new Map();
      this.waiting.set(poolKey, pool);
    }
    pool.set(participant.id, { participantId: participant.id, poolKey, joinedAt: participant.joinedAt, version: participant.version });
  }
}
