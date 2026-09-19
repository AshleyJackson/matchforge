import { FilterOperandSchema, SerializableFilterSchema } from "./schemas";
import {
  ParticipantPreferencesSchema,
  ParticipantSchema,
} from "./schemas";
import { z } from "zod";

export function evaluateSerializableFilter(
  filter: unknown,
  requester: z.infer<typeof ParticipantSchema>,
  candidate: z.infer<typeof ParticipantSchema>,
): boolean {
  const validated = SerializableFilterSchema.parse(filter);
  return evaluate(validated, requester.preferences, candidate.preferences);
}

function evaluate(
  filter: unknown,
  requester: z.infer<typeof ParticipantPreferencesSchema>,
  candidate: z.infer<typeof ParticipantPreferencesSchema>,
): boolean {
  const node = z.record(z.string(), z.unknown()).parse(filter);
  const operation = z.enum(["all", "any", "not", "overlaps", "equals", "in", "contains", "is-empty"]).parse(node.op);
  switch (operation) {
    case "all":
      return z.array(z.unknown()).parse(node.filters ?? []).every((nested) =>
        evaluate(nested, requester, candidate),
      );
    case "any":
      return z.array(z.unknown()).parse(node.filters ?? []).some((nested) =>
        evaluate(nested, requester, candidate),
      );
    case "not":
      return !evaluate(node.filter, requester, candidate);
    case "overlaps": {
      const left = readOperand(node.left, requester, candidate);
      const right = readOperand(node.right, requester, candidate);
      if (!Array.isArray(left) || !Array.isArray(right)) return false;
      return left.some((value) => right.some((other) => JSON.stringify(value) === JSON.stringify(other)));
    }
    case "equals": {
      return JSON.stringify(readOperand(node.left, requester, candidate)) ===
        JSON.stringify(readOperand(node.right, requester, candidate));
    }
    case "in": {
      const value = readOperand(node.left, requester, candidate);
      const candidates = readOperand(node.right, requester, candidate);
      return Array.isArray(candidates) &&
        candidates.some((candidateValue) => JSON.stringify(value) === JSON.stringify(candidateValue));
    }
    case "contains": {
      const container = readOperand(node.left, requester, candidate);
      const value = readOperand(node.right, requester, candidate);
      if (Array.isArray(container)) {
        return container.some((item) => JSON.stringify(item) === JSON.stringify(value));
      }
      return typeof container === "string" && typeof value === "string" && container.includes(value);
    }
    case "is-empty": {
      const value = readOperand(node.left, requester, candidate);
      return value == null ||
        (typeof value === "string" && value.length === 0) ||
        (Array.isArray(value) && value.length === 0) ||
        (typeof value === "object" &&
          !Array.isArray(value) &&
          value !== null &&
          Object.keys(value).length === 0);
    }
    default:
      return false;
  }
}

function readOperand(
  operand: unknown,
  requester: z.infer<typeof ParticipantPreferencesSchema>,
  candidate: z.infer<typeof ParticipantPreferencesSchema>,
): unknown {
  const parsed = FilterOperandSchema.parse(operand);
  if ("literal" in parsed) return parsed.literal;
  let value: unknown = parsed.source === "requester" ? requester : candidate;
  for (const segment of parsed.path) {
    if (!value || typeof value !== "object") return undefined;
    value = (value as Record<string, unknown>)[segment];
  }
  return value;
}
