import { SerializableFilterSchema } from "./schemas";
import type { Participant, ParticipantPreferences, SerializableFilter } from "./types";

type FilterOperand = SerializableFilter["left"] extends infer Operand ? Operand : never;

export function evaluateSerializableFilter(
  filter: SerializableFilter,
  requester: Participant,
  candidate: Participant,
): boolean {
  const validated = SerializableFilterSchema.parse(filter);
  return evaluate(validated, requester.preferences, candidate.preferences);
}

function evaluate(
  filter: SerializableFilter,
  requester: ParticipantPreferences,
  candidate: ParticipantPreferences,
): boolean {
  switch (filter.op) {
    case "all":
      return (filter.filters ?? []).every((nested) => evaluate(nested, requester, candidate));
    case "any":
      return (filter.filters ?? []).some((nested) => evaluate(nested, requester, candidate));
    case "not":
      return !evaluate(filter.filter!, requester, candidate);
    case "overlaps": {
      const left = readOperand(filter.left!, requester, candidate);
      const right = readOperand(filter.right!, requester, candidate);
      if (!Array.isArray(left) || !Array.isArray(right)) return false;
      return left.some((value) => right.some((other) => JSON.stringify(value) === JSON.stringify(other)));
    }
  }
}

function readOperand(
  operand: NonNullable<FilterOperand>,
  requester: ParticipantPreferences,
  candidate: ParticipantPreferences,
): unknown {
  if ("literal" in operand) return operand.literal;
  let value: unknown = operand.source === "requester" ? requester : candidate;
  for (const segment of operand.path) {
    if (!value || typeof value !== "object") return undefined;
    value = (value as Record<string, unknown>)[segment];
  }
  return value;
}
