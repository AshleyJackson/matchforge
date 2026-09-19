import { ParticipantPreferencesSchema, ParticipantSchema, MatchClaimSchema } from "./schemas";
import { z } from "zod";

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
    ? { ...value, pools: [...value.pools], preferences: clonePreferences(value.preferences) }
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
