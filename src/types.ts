export type ParticipantId = string;
export type PoolKey = string;
export type ClaimId = string;
export type MatchId = string;
export type IdempotencyKey = string;

export interface ParticipantPreferences {
  [key: string]: unknown;
}

export type ParticipantState =
  | "created" | "waiting" | "claimed" | "matched"
  | "away" | "cancelled" | "expired" | "left";

export interface Participant {
  id: ParticipantId;
  preferences: ParticipantPreferences;
  version: number;
  state: ParticipantState;
  joinedAt: number;
  lastHeartbeatAt: number;
  pools: PoolKey[];
  metadata?: Record<string, unknown>;
}

export interface WaitingEntry {
  participantId: ParticipantId;
  poolKey: PoolKey;
  scoreHint?: number;
  joinedAt: number;
  version: number;
}

export interface MatchClaim {
  claimId: ClaimId;
  matchId: MatchId;
  poolKey: PoolKey;
  participants: Array<{
    id: ParticipantId;
    version: number;
    snapshot: ParticipantPreferences;
  }>;
  state: "reserved" | "committed" | "expired" | "rolled_back";
  createdAt: number;
  expiresAt: number;
  strategy: string;
  recoveryToken: string;
}

export interface MatchRecord {
  matchId: MatchId;
  claimId: ClaimId;
  poolKey: PoolKey;
  participantIds: ParticipantId[];
  createdAt: number;
  status: "active" | "ended" | "failed";
}

export interface MatchContext {
  now: number;
  waitTimeMs: number;
  poolKey: PoolKey;
  attempt: number;
}

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

export interface RelaxationPolicy {
  stages: Array<{
    afterMs: number;
    relax: (preferences: ParticipantPreferences) => ParticipantPreferences;
  }>;
}

export interface ScoredCandidate {
  participant: Participant;
  score: number;
  entry: WaitingEntry;
}

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
  claimTtlMs: number;
  claimId: ClaimId;
  matchId: MatchId;
  idempotencyKey?: IdempotencyKey;
  random: () => number;
}

export interface CoordinationStore {
  atomicClaim(input: AtomicClaimInput): Promise<ClaimResult>;
  putParticipant(participant: Participant): Promise<void>;
  getParticipant(id: ParticipantId): Promise<Participant | null>;
  updateParticipant(id: ParticipantId, update: (participant: Participant) => Participant): Promise<Participant | null>;
  addToWaitingIndex(entry: WaitingEntry): Promise<void>;
  removeFromWaitingIndex(poolKey: PoolKey, participantId: ParticipantId): Promise<void>;
  getClaim(claimId: ClaimId): Promise<MatchClaim | null>;
  putClaim(claim: MatchClaim): Promise<void>;
  getQueueDepth(poolKey: PoolKey): Promise<number>;
  recoverExpiredClaims(now: number): Promise<MatchClaim[]>;
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
  idempotencyKey?: IdempotencyKey;
  claimTtlMs?: number;
}

export type RegisterParticipantInput =
  Omit<Participant, "version" | "state" | "joinedAt" | "lastHeartbeatAt" | "pools"> & {
    pools?: PoolKey[];
  };
