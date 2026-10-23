import { z } from "zod";

export const IdentifierSchema = z.string().trim().min(1).max(256);
export const JsonPathSchema = z.array(IdentifierSchema).max(32);
export const FilterSourceSchema = z.enum(["requester", "candidate"]);
export const FilterOperandSchema = z.union([
  z.object({ source: FilterSourceSchema, path: JsonPathSchema }).strict(),
  z.object({ literal: z.unknown() }).strict(),
]);

/**
 * Domain-neutral filter AST. The adapter only understands JSON paths and
 * operators; it does not know whether a path represents language, interests,
 * rank, or any other application concept.
 */
export const SerializableFilterSchema = z.lazy((): z.ZodTypeAny => z.object({
  op: z.enum(["all", "any", "not", "overlaps", "equals", "in", "contains", "is-empty"]),
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
  if (
    ["overlaps", "equals", "in", "contains"].includes(value.op) &&
    (!value.left || !value.right)
  ) {
    context.addIssue({ code: "custom", path: ["left"], message: `${value.op} requires left and right operands` });
  }
  if (value.op === "is-empty" && !value.left) {
    context.addIssue({ code: "custom", path: ["left"], message: "is-empty requires a left operand" });
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
  requestFingerprint: z.string().min(1).max(4096),
}).strict();

export const MatchRecordSchema = z.object({
  matchId: IdentifierSchema,
  claimId: IdentifierSchema,
  poolKey: IdentifierSchema,
  participantIds: z.array(IdentifierSchema).length(2),
  createdAt: z.number().finite(),
  status: z.enum(["active", "ended", "failed"]),
}).strict();

export const RelaxationPolicySchema = z.object({
  stages: z.array(z.object({
    afterMs: z.number().finite().nonnegative(),
    relax: z.function({
      input: [ParticipantPreferencesSchema],
      output: ParticipantPreferencesSchema,
    }),
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
  compatibility: z.function().optional(),
  score: z.function().optional(),
  relax: RelaxationPolicySchema.optional(),
});

export const PoolListSchema = z.array(IdentifierSchema).length(1);

export const MatchContextSchema = z.object({
  now: z.number().finite(),
  waitTimeMs: z.number().finite().nonnegative(),
  poolKey: IdentifierSchema,
  attempt: z.number().int().nonnegative(),
}).strict();

export const ScoredCandidateSchema = z.object({
  participant: ParticipantSchema,
  score: z.number().finite(),
  entry: WaitingEntrySchema,
}).strict();

export const SerializableSelectionStrategySchema = z.discriminatedUnion("type", [
  z.object({ type: z.literal("fifo") }).strict(),
  z.object({ type: z.literal("random") }).strict(),
  z.object({ type: z.literal("best-score"), higherIsBetter: z.boolean().optional() }).strict(),
  z.object({ type: z.literal("weighted-random") }).strict(),
  z.object({
    type: z.literal("custom"),
    select: z.function({
      input: [z.array(ScoredCandidateSchema)],
      output: ScoredCandidateSchema.nullable(),
    }),
  }).strict(),
]);

export const CompatibilityFunctionSchema = z.function({
  input: [ParticipantSchema, ParticipantSchema, MatchContextSchema],
  output: z.union([z.boolean(), z.promise(z.boolean())]),
});

export const ScoreFunctionSchema = z.function({
  input: [ParticipantSchema, ParticipantSchema, MatchContextSchema],
  output: z.union([z.number().finite(), z.null(), z.promise(z.union([z.number().finite(), z.null()]))]),
});

export const FindMatchInputSchemaWithFunctions = FindMatchInputSchema.extend({
  strategy: SerializableSelectionStrategySchema.optional(),
  compatibility: CompatibilityFunctionSchema.optional(),
  score: ScoreFunctionSchema.optional(),
});

export const RegisterParticipantInputSchema = z.object({
  id: IdentifierSchema,
  preferences: ParticipantPreferencesSchema,
  metadata: z.record(z.string(), z.unknown()).optional(),
  pools: PoolListSchema.optional(),
}).strict();

export const AtomicClaimInputSchema = z.object({
  requesterId: IdentifierSchema,
  poolKey: IdentifierSchema,
  now: z.number().finite(),
  scanLimit: z.number().int().positive(),
  excludeIds: z.array(IdentifierSchema),
  strategy: SerializableSelectionStrategySchema,
  compatibility: CompatibilityFunctionSchema.optional(),
  score: ScoreFunctionSchema.optional(),
  relax: RelaxationPolicySchema.optional(),
  filter: SerializableFilterSchema.optional(),
  claimTtlMs: z.number().int().positive(),
  claimId: IdentifierSchema,
  matchId: IdentifierSchema,
  idempotencyKey: IdentifierSchema.optional(),
  requestFingerprint: z.string().min(1).max(4096).optional(),
  random: z.function({ output: z.number().min(0).max(1) }),
}).strict();

export const ClaimResultSchema = z.discriminatedUnion("status", [
  z.object({
    status: z.literal("matched"),
    claim: MatchClaimSchema,
    scanned: z.number().int().nonnegative().optional(),
    stale: z.number().int().nonnegative().optional(),
  }).strict(),
  z.object({
    status: z.literal("none"),
    scanned: z.number().int().nonnegative().optional(),
    stale: z.number().int().nonnegative().optional(),
  }).strict(),
  z.object({ status: z.literal("stale"), reason: z.string() }).strict(),
  z.object({ status: z.literal("error"), error: z.instanceof(Error) }).strict(),
]);

const AsyncUnitFunctionSchema = z.function({
  input: [IdentifierSchema],
  output: z.promise(z.void()),
});

export const CoordinationStoreSchema = z.object({
  atomicClaim: z.function({
    input: [AtomicClaimInputSchema],
    output: z.promise(ClaimResultSchema),
  }),
  putParticipant: z.function({
    input: [ParticipantSchema],
    output: z.promise(z.void()),
  }),
  getParticipant: z.function({
    input: [IdentifierSchema],
    output: z.promise(ParticipantSchema.nullable()),
  }),
  updateParticipant: z.function({
    input: [
      IdentifierSchema,
      z.function({
        input: [ParticipantSchema],
        output: ParticipantSchema,
      }),
    ],
    output: z.promise(ParticipantSchema.nullable()),
  }),
  addToWaitingIndex: z.function({
    input: [WaitingEntrySchema],
    output: z.promise(z.void()),
  }),
  removeFromWaitingIndex: z.function({
    input: [IdentifierSchema, IdentifierSchema],
    output: z.promise(z.void()),
  }),
  getClaim: z.function({
    input: [IdentifierSchema],
    output: z.promise(MatchClaimSchema.nullable()),
  }),
  putClaim: z.function({
    input: [MatchClaimSchema],
    output: z.promise(z.void()),
  }),
  commitClaim: z.function({
    input: [IdentifierSchema],
    output: z.promise(z.boolean()),
  }),
  rollbackClaim: z.function({
    input: [IdentifierSchema],
    output: z.promise(z.boolean()),
  }),
  clearIdempotency: AsyncUnitFunctionSchema,
  getQueueDepth: z.function({
    input: [IdentifierSchema],
    output: z.promise(z.number().int().nonnegative()),
  }),
  recoverExpiredClaims: z.function({
    input: [
      z.number(),
      z.function({
        input: [MatchClaimSchema],
        output: z.promise(z.boolean()),
      }).optional(),
    ],
    output: z.promise(z.array(MatchClaimSchema)),
  }),
}).strict();

export const PersistenceStoreSchema = z.object({
  saveMatch: z.function({ input: [MatchRecordSchema], output: z.promise(z.void()) }),
  getMatch: z.function({ input: [IdentifierSchema], output: z.promise(MatchRecordSchema.nullable()) }),
  completeMatch: z.function({ input: [IdentifierSchema], output: z.promise(z.void()) }),
}).strict();

export const MetricsSinkSchema = z.object({
  increment: z.function(),
  histogram: z.function(),
  gauge: z.function(),
}).strict();

export const LoggerSchema = z.object({
  debug: z.function().optional(),
  warn: z.function().optional(),
  error: z.function().optional(),
}).strict();

export const MatcherOptionsSchema = z.object({
  store: CoordinationStoreSchema,
  persistence: PersistenceStoreSchema.optional(),
  clock: z.function({ output: z.number() }).optional(),
  idGenerator: z.function({ output: z.string() }).optional(),
  random: z.function({ output: z.number() }).optional(),
  logger: LoggerSchema.optional(),
  metrics: MetricsSinkSchema.optional(),
  defaultClaimTtlMs: z.number().int().positive().optional(),
  defaultScanLimit: z.number().int().positive().optional(),
  heartbeatTimeoutMs: z.number().int().positive().optional(),
}).strict();
