import {
  ClaimCursorSchema,
  MatchClaimSchema,
  ParticipantPreferencesSchema,
  ParticipantSchema,
  SyncParticipantInputSchema,
} from "./schemas";
import { z } from "zod";

/**
 * Default request fingerprint for idempotent claims. It covers the inputs that
 * change which candidate can be chosen, and omits per-attempt values such as
 * `claimId`, `now`, and `cursor`, so one key can span every page of a search.
 */
export function claimRequestFingerprint(input: {
  requesterId: string;
  poolKey: string;
  scanLimit: number;
  excludeIds: string[];
  strategy: unknown;
  filter?: unknown;
  maxHeartbeatAgeMs?: number;
}): string {
  return stableSerialize({
    requesterId: input.requesterId,
    poolKey: input.poolKey,
    scanLimit: input.scanLimit,
    excludeIds: input.excludeIds,
    strategy: input.strategy,
    filter: input.filter,
    ...(input.maxHeartbeatAgeMs !== undefined
      ? { maxHeartbeatAgeMs: input.maxHeartbeatAgeMs }
      : {}),
  });
}

/** Claim cursors are `<joinedAt>:<participantId>` for the last entry examined. */
export function encodeClaimCursor(joinedAt: number | string, participantId: string): string {
  return ClaimCursorSchema.parse(`${joinedAt}:${participantId}`);
}

export function decodeClaimCursor(cursor: string): { joinedAt: number; participantId: string } {
  const separator = ClaimCursorSchema.parse(cursor).indexOf(":");
  const joinedAt = Number(cursor.slice(0, separator));
  const participantId = cursor.slice(separator + 1);
  if (separator <= 0 || !Number.isFinite(joinedAt) || participantId.length === 0) {
    throw new Error("invalid claim cursor");
  }
  return { joinedAt, participantId };
}

/** The record `syncParticipant()` writes, minus the version the store assigns. */
export function syncedParticipantBody(
  input: z.infer<typeof SyncParticipantInputSchema>,
): Omit<z.infer<typeof ParticipantSchema>, "version"> {
  return {
    id: input.id,
    preferences: clonePreferences(input.preferences),
    state: input.state,
    joinedAt: input.joinedAt,
    lastHeartbeatAt: input.lastHeartbeatAt,
    pools: [...input.pools],
    ...(input.metadata !== undefined ? { metadata: input.metadata } : {}),
    ...(input.exclusions !== undefined ? { exclusions: { ...input.exclusions } } : {}),
    ...(input.revision !== undefined ? { syncRevision: input.revision } : {}),
  };
}

/** `true` when `owner` excludes `otherId` at `now`. */
export function isExcluded(
  owner: z.infer<typeof ParticipantSchema>,
  otherId: string,
  now: number,
): boolean {
  const until = owner.exclusions?.[otherId];
  return until !== undefined && until > now;
}

export function stableSerialize(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(stableSerialize).join(",")}]`;
  if (value && typeof value === "object") {
    return `{${Object.keys(value as Record<string, unknown>)
      .sort()
      .map((key) => `${JSON.stringify(key)}:${stableSerialize((value as Record<string, unknown>)[key])}`)
      .join(",")}}`;
  }
  return JSON.stringify(value);
}

export function clonePreferences(
  value: z.infer<typeof ParticipantPreferencesSchema>,
): z.infer<typeof ParticipantPreferencesSchema> {
  return ParticipantPreferencesSchema.parse(JSON.parse(JSON.stringify(value)));
}

export function cloneParticipant(
  value: z.infer<typeof ParticipantSchema> | undefined | null,
): z.infer<typeof ParticipantSchema> | null {
  return value
    ? {
        ...value,
        pools: [...value.pools],
        preferences: clonePreferences(value.preferences),
        ...(value.exclusions ? { exclusions: { ...value.exclusions } } : {}),
      }
    : null;
}

export function cloneClaim(
  value: z.infer<typeof MatchClaimSchema> | undefined | null,
): z.infer<typeof MatchClaimSchema> | null {
  return value
    ? {
        ...value,
        participants: value.participants.map((participant) => ({
          ...participant,
          snapshot: clonePreferences(participant.snapshot),
        })),
      }
    : null;
}
