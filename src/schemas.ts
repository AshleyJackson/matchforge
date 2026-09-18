import { z } from "zod";

export const IdentifierSchema = z.string().trim().min(1).max(256);

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
});

export const PoolListSchema = z.array(IdentifierSchema).max(16);
