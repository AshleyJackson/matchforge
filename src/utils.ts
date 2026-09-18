import type { MatchClaim, Participant, ParticipantPreferences } from "./types.ts";

export function clonePreferences(value: ParticipantPreferences): ParticipantPreferences {
  return JSON.parse(JSON.stringify(value)) as ParticipantPreferences;
}

export function cloneParticipant(value: Participant | undefined | null): Participant | null {
  return value
    ? { ...value, pools: [...value.pools], preferences: clonePreferences(value.preferences) }
    : null;
}

export function cloneClaim(value: MatchClaim | undefined | null): MatchClaim | null {
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
