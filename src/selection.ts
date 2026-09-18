import type {
  Participant,
  RelaxationPolicy,
  ScoredCandidate,
  SelectionStrategy,
} from "./types";
import { cloneParticipant, clonePreferences } from "./utils";

export function selectCandidate(
  candidates: ScoredCandidate[],
  strategy: SelectionStrategy,
  random: () => number,
): ScoredCandidate | null {
  if (candidates.length === 0) return null;
  if (strategy.type === "fifo") return candidates[0]!;
  if (strategy.type === "random") return candidates[Math.min(candidates.length - 1, Math.floor(random() * candidates.length))]!;
  if (strategy.type === "best-score") {
    return candidates.reduce((best, candidate) =>
      strategy.higherIsBetter === false
        ? (candidate.score < best.score ? candidate : best)
        : (candidate.score > best.score ? candidate : best));
  }
  if (strategy.type === "custom") return strategy.select(candidates);
  const total = candidates.reduce((sum, candidate) => sum + Math.max(0, candidate.score), 0);
  if (total <= 0) return candidates[Math.min(candidates.length - 1, Math.floor(random() * candidates.length))]!;
  let target = random() * total;
  for (const candidate of candidates) {
    target -= Math.max(0, candidate.score);
    if (target <= 0) return candidate;
  }
  return candidates[candidates.length - 1]!;
}

export function applyRelaxation(
  participant: Participant,
  policy: RelaxationPolicy | undefined,
  now: number,
): Participant {
  if (!policy) return cloneParticipant(participant)!;
  let preferences = clonePreferences(participant.preferences);
  for (const stage of [...policy.stages].sort((a, b) => a.afterMs - b.afterMs)) {
    if (now - participant.joinedAt >= stage.afterMs) preferences = stage.relax(preferences);
  }
  return { ...cloneParticipant(participant)!, preferences };
}
