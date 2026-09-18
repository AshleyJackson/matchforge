import type {
  AtomicClaimInput, ClaimResult, CoordinationStore, MatchClaim, Participant,
  ParticipantId, PoolKey, WaitingEntry,
} from "./types";
import { applyRelaxation, selectCandidate } from "./selection";
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
  private readonly executor = new SerialExecutor();

  async putParticipant(participant: Participant): Promise<void> {
    await this.executor.run(() => this.participants.set(participant.id, cloneParticipant(participant)!));
  }

  async getParticipant(id: ParticipantId): Promise<Participant | null> {
    return this.executor.run(() => cloneParticipant(this.participants.get(id)));
  }

  async updateParticipant(id: ParticipantId, update: (participant: Participant) => Participant): Promise<Participant | null> {
    return this.executor.run(() => {
      const current = this.participants.get(id);
      if (!current) return null;
      const next = update(cloneParticipant(current)!);
      this.participants.set(id, cloneParticipant(next)!);
      return cloneParticipant(next);
    });
  }

  async addToWaitingIndex(entry: WaitingEntry): Promise<void> {
    await this.executor.run(() => {
      let pool = this.waiting.get(entry.poolKey);
      if (!pool) {
        pool = new Map();
        this.waiting.set(entry.poolKey, pool);
      }
      pool.set(entry.participantId, { ...entry });
    });
  }

  async removeFromWaitingIndex(poolKey: PoolKey, participantId: ParticipantId): Promise<void> {
    await this.executor.run(() => this.waiting.get(poolKey)?.delete(participantId));
  }

  async getClaim(claimId: string): Promise<MatchClaim | null> {
    return this.executor.run(() => cloneClaim(this.claims.get(claimId)));
  }

  async putClaim(claim: MatchClaim): Promise<void> {
    await this.executor.run(() => this.claims.set(claim.claimId, cloneClaim(claim)!));
  }

  async getQueueDepth(poolKey: PoolKey): Promise<number> {
    return this.executor.run(() => this.waiting.get(poolKey)?.size ?? 0);
  }

  async atomicClaim(input: AtomicClaimInput): Promise<ClaimResult> {
    return this.executor.run(async () => {
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
      return { status: "matched", claim: cloneClaim(claim)! };
    });
  }

  async recoverExpiredClaims(now: number): Promise<MatchClaim[]> {
    return this.executor.run(() => {
      const expired: MatchClaim[] = [];
      for (const claim of this.claims.values()) {
        if (claim.state !== "reserved" || claim.expiresAt > now) continue;
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
