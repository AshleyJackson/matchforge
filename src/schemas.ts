import { z } from "zod";

export const IdentifierSchema = z.string().trim().min(1).max(256);
export const JsonPathSchema = z.array(IdentifierSchema).max(32);
export const FilterSourceSchema = z.enum(["requester", "candidate"]);
export const FilterOperandSchema = z.union([
  z.object({ source: FilterSourceSchema, path: JsonPathSchema }).strict(),
  z.object({ literal: z.unknown() }).strict(),
]);

type FilterOperand = {
  source: "requester" | "candidate";
  path: string[];
} | {
  literal: unknown;
};

type FilterNode = {
  op: "all" | "any" | "not" | "overlaps";
  filters?: FilterNode[];
  filter?: FilterNode;
  left?: FilterOperand;
  right?: FilterOperand;
};

/**
 * Domain-neutral filter AST. The adapter only understands JSON paths and
 * operators; it does not know whether a path represents language, interests,
 * rank, or any other application concept.
 */
export const SerializableFilterSchema: z.ZodType<FilterNode> = z.lazy(() => z.object({
  op: z.enum(["all", "any", "not", "overlaps"]),
  filters: z.array(z.lazy(() => SerializableFilterSchema)).max(32).optional(),
  filter: z.lazy(() => SerializableFilterSchema).optional(),
  left: FilterOperandSchema.optional(),
  right: FilterOperandSchema.optional(),
}).strict().superRefine((value, context) => {
  if ((value.op === "all" || value.op === "any") && (!value.filters || value.filters.length === 0)) {
    context.addIssue({ code: "custom", path: ["filters"], message: `${value.op} requires at least one filter` });
  }
  if (value.op === "not" && !value.filter) {
    context.addIssue({ code: "custom", path: ["filter"], message: "not requires a nested filter" });
  }
  if (value.op === "overlaps" && (!value.left || !value.right)) {
    context.addIssue({ code: "custom", path: ["left"], message: "overlaps requires left and right operands" });
  }
}));

export const ParticipantPreferencesSchema = z.record(z.string(), z.unknown()).superRefine((preferences, context) => {
  try {
    const bytes = new TextEncoder().encode(JSON.stringify(preferences)).byteLength;
    if (bytes > 8 * 1024) {
      context.addIssue({ code: "custom", message: "preferences must be 8 KiB or smaller" });
    }
  } catch {
    context.addIssue({ code: "custom", message: "preferences must be JSON serializable" });
  }
});

export const ParticipantStateSchema = z.enum([
  "created", "waiting", "claimed", "matched",
  "away", "cancelled", "expired", "left",
]);

export const ParticipantSchema = z.object({
  id: IdentifierSchema,
  preferences: ParticipantPreferencesSchema,
  version: z.number().int().nonnegative(),
  state: ParticipantStateSchema,
  joinedAt: z.number().finite(),
  lastHeartbeatAt: z.number().finite(),
  pools: z.array(IdentifierSchema).max(16),
  metadata: z.record(z.string(), z.unknown()).optional(),
}).strict();

export const WaitingEntrySchema = z.object({
  participantId: IdentifierSchema,
  poolKey: IdentifierSchema,
  scoreHint: z.number().finite().optional(),
  joinedAt: z.number().finite(),
  version: z.number().int().nonnegative(),
}).strict();

export const MatchClaimParticipantSchema = z.object({
  id: IdentifierSchema,
  version: z.number().int().nonnegative(),
  snapshot: ParticipantPreferencesSchema,
}).strict();

export const MatchClaimSchema = z.object({
  claimId: IdentifierSchema,
  matchId: IdentifierSchema,
  poolKey: IdentifierSchema,
  participants: z.array(MatchClaimParticipantSchema).length(2),
  state: z.enum(["reserved", "committed", "expired", "rolled_back"]),
  createdAt: z.number().finite(),
  expiresAt: z.number().finite(),
  strategy: IdentifierSchema,
  recoveryToken: IdentifierSchema,
}).strict();

export const MatchRecordSchema = z.object({
  matchId: IdentifierSchema,
  claimId: IdentifierSchema,
  poolKey: IdentifierSchema,
  participantIds: z.array(IdentifierSchema).length(2),
  createdAt: z.number().finite(),
  status: z.enum(["active", "ended", "failed"]),
}).strict();

export const MatchContextSchema = z.object({
  now: z.number().finite(),
  waitTimeMs: z.number().finite().nonnegative(),
  poolKey: IdentifierSchema,
  attempt: z.number().int().nonnegative(),
}).strict();

export const RelaxationPolicySchema = z.object({
  stages: z.array(z.object({
    afterMs: z.number().finite().nonnegative(),
    relax: z.custom<(preferences: Record<string, unknown>) => Record<string, unknown>>(
      (value) => typeof value === "function",
    ),
  }).strict()).max(32),
}).strict();

export const RegisterParticipantSchema = z.object({
  id: IdentifierSchema,
  preferences: ParticipantPreferencesSchema,
  metadata: z.record(z.string(), z.unknown()).optional(),
  pools: z.array(IdentifierSchema).max(16).optional(),
});

export const FindMatchInputSchema = z.object({
  requesterId: IdentifierSchema,
  poolKey: IdentifierSchema,
  scanLimit: z.number().int().min(1).max(10_000).optional(),
  excludeIds: z.array(IdentifierSchema).max(1_000).optional(),
  idempotencyKey: IdentifierSchema.optional(),
  claimTtlMs: z.number().int().min(1_000).max(24 * 60 * 60 * 1_000).optional(),
  filter: SerializableFilterSchema.optional(),
  compatibility: z.custom<(...args: unknown[]) => unknown>((value) => typeof value === "function").optional(),
  score: z.custom<(...args: unknown[]) => unknown>((value) => typeof value === "function").optional(),
  relax: RelaxationPolicySchema.optional(),
});

export const PoolListSchema = z.array(IdentifierSchema).max(16);

export const SerializableSelectionStrategySchema = z.object({
  type: z.literal("fifo"),
}).strict();
