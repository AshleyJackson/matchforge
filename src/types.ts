import { z } from "zod";
import {
  FindMatchInputSchema,
  MatchContextSchema,
  MatchClaimSchema,
  MatchRecordSchema,
  ParticipantSchema,
  ParticipantPreferencesSchema,
  ParticipantStateSchema,
  RelaxationPolicySchema,
  SerializableFilterSchema,
  SerializableSelectionStrategySchema,
  WaitingEntrySchema,
} from "./schemas";

export type ParticipantId = string;
export type PoolKey = string;
export type ClaimId = string;
export type MatchId = string;
export type IdempotencyKey = string;

export type ParticipantPreferences = z.infer<typeof ParticipantPreferencesSchema>;
export type SerializableFilter = z.infer<typeof SerializableFilterSchema>;
export type SerializableSelectionStrategy = z.infer<typeof SerializableSelectionStrategySchema>;

export type ParticipantState = z.infer<typeof ParticipantStateSchema>;
export type Participant = z.infer<typeof ParticipantSchema>;
export type WaitingEntry = z.infer<typeof WaitingEntrySchema>;
export type MatchClaim = z.infer<typeof MatchClaimSchema>;
export type MatchRecord = z.infer<typeof MatchRecordSchema>;

export type MatchContext = z.infer<typeof MatchContextSchema>;

export type CompatibilityFn = (
  requester: Participant,
  candidate: Participant,
  context: MatchContext,
) => boolean | Promise<boolean>;

export type ScoreFn = (
  requester: Participant,
  candidate: Participant,
  context: MatchContext,
) => number | null | Promise<number | null>;

export type RelaxationPolicy = z.infer<typeof RelaxationPolicySchema>;

export type ScoredCandidate = {
  participant: Participant;
  score: number;
  entry: WaitingEntry;
};

export type SelectionStrategy =
  | { type: "fifo" }
  | { type: "random" }
  | { type: "best-score"; higherIsBetter?: boolean }
  | { type: "weighted-random" }
  | { type: "custom"; select: (candidates: ScoredCandidate[]) => ScoredCandidate | null };

export type ClaimResult =
  | { status: "matched"; claim: MatchClaim }
  | { status: "none" }
  | { status: "stale"; reason: string }
  | { status: "error"; error: Error };

export interface AtomicClaimInput {
  requesterId: ParticipantId;
  poolKey: PoolKey;
  now: number;
  scanLimit: number;
  excludeIds: ParticipantId[];
  strategy: SelectionStrategy;
  compatibility?: CompatibilityFn;
  score?: ScoreFn;
  relax?: RelaxationPolicy;
  filter?: SerializableFilter;
  claimTtlMs: number;
  claimId: ClaimId;
  matchId: MatchId;
  idempotencyKey?: IdempotencyKey;
  random: () => number;
}

export type FindMatchInput = z.infer<typeof FindMatchInputSchema>;

export interface CoordinationStore {
  atomicClaim(input: AtomicClaimInput): Promise<ClaimResult>;
  putParticipant(participant: Participant): Promise<void>;
  getParticipant(id: ParticipantId): Promise<Participant | null>;
  updateParticipant(id: ParticipantId, update: (participant: Participant) => Participant): Promise<Participant | null>;
  addToWaitingIndex(entry: WaitingEntry): Promise<void>;
  removeFromWaitingIndex(poolKey: PoolKey, participantId: ParticipantId): Promise<void>;
  getClaim(claimId: ClaimId): Promise<MatchClaim | null>;
  putClaim(claim: MatchClaim): Promise<void>;
  commitClaim(claimId: ClaimId): Promise<boolean>;
  rollbackClaim(claimId: ClaimId): Promise<boolean>;
  clearIdempotency(key: IdempotencyKey): Promise<void>;
  getQueueDepth(poolKey: PoolKey): Promise<number>;
  recoverExpiredClaims(
    now: number,
    hasDurableMatch?: (claim: MatchClaim) => Promise<boolean>,
  ): Promise<MatchClaim[]>;
}

export interface PersistenceStore {
  saveMatch(record: MatchRecord): Promise<void>;
  getMatch(matchId: MatchId): Promise<MatchRecord | null>;
  completeMatch(matchId: MatchId): Promise<void>;
}

export interface MetricsSink {
  increment(name: string, tags?: Record<string, string>): void;
  histogram(name: string, value: number, tags?: Record<string, string>): void;
  gauge(name: string, value: number, tags?: Record<string, string>): void;
}

export interface Logger {
  debug?(message: string, fields?: Record<string, unknown>): void;
  warn?(message: string, fields?: Record<string, unknown>): void;
  error?(message: string, fields?: Record<string, unknown>): void;
}

export interface MatcherOptions {
  store: CoordinationStore;
  persistence?: PersistenceStore;
  clock?: () => number;
  idGenerator?: () => string;
  random?: () => number;
  logger?: Logger;
  metrics?: MetricsSink;
  defaultClaimTtlMs?: number;
  defaultScanLimit?: number;
  heartbeatTimeoutMs?: number;
}

export interface FindMatchOptions {
  requesterId: ParticipantId;
  poolKey: PoolKey;
  strategy?: SelectionStrategy;
  scanLimit?: number;
  excludeIds?: ParticipantId[];
  compatibility?: CompatibilityFn;
  score?: ScoreFn;
  relax?: RelaxationPolicy;
  filter?: SerializableFilter;
  idempotencyKey?: IdempotencyKey;
  claimTtlMs?: number;
}

export type RegisterParticipantInput =
  Omit<Participant, "version" | "state" | "joinedAt" | "lastHeartbeatAt" | "pools"> & {
    pools?: PoolKey[];
  };
