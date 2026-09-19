import { z } from "zod";
import {
  FilterOperandSchema,
  JsonPathSchema,
  SerializableFilterSchema,
} from "./schemas";

/**
 * Describes a symmetric compatibility rule without naming a domain concept.
 *
 * `equals` is useful for scalar preferences such as locale or category.
 * `overlaps` is useful for collections such as tags or interests.
 * Wildcard values make either participant's scalar preference unconstrained.
 * Empty collections can likewise be configured as unconstrained.
 */
export const MutualCompatibilityRuleSchema = z.object({
  mode: z.enum(["equals", "overlaps"]),
  requesterPath: JsonPathSchema,
  candidatePath: JsonPathSchema,
  wildcardValues: z.array(z.unknown()).max(32).optional(),
  emptyMatchesAll: z.boolean().optional(),
}).strict();

export const MutualCompatibilitySchema = z.object({
  rules: z.array(MutualCompatibilityRuleSchema).min(1).max(32),
}).strict();

function operand(source: "requester" | "candidate", path: string[]) {
  return FilterOperandSchema.parse({ source, path });
}

function literal(value: unknown) {
  return FilterOperandSchema.parse({ literal: value });
}

function wildcardClause(
  source: "requester" | "candidate",
  path: string[],
  values: unknown[],
) {
  return {
    op: "in",
    left: operand(source, path),
    right: literal(values),
  };
}

/**
 * Builds a serializable, adapter-neutral filter for symmetric compatibility.
 *
 * For every rule, both participants must be compatible. Scalar wildcard values
 * and empty collections are represented in the AST, so Redis and in-memory
 * adapters evaluate the same behavior.
 */
export function buildMutualCompatibilityFilter(
  input: z.input<typeof MutualCompatibilitySchema>,
) {
  const validated = MutualCompatibilitySchema.parse(input);
  const filters = validated.rules.map((rule) => {
    const direct =
      rule.mode === "equals"
        ? {
            op: "equals",
            left: operand("requester", rule.requesterPath),
            right: operand("candidate", rule.candidatePath),
          }
        : {
            op: "overlaps",
            left: operand("requester", rule.requesterPath),
            right: operand("candidate", rule.candidatePath),
          };

    const alternatives = z.array(z.unknown()).parse([direct]);
    if (rule.mode === "equals" && rule.wildcardValues?.length) {
      alternatives.push(
        wildcardClause("requester", rule.requesterPath, rule.wildcardValues),
        wildcardClause("candidate", rule.candidatePath, rule.wildcardValues),
      );
    }
    if (rule.mode === "overlaps" && rule.emptyMatchesAll) {
      alternatives.push(
        {
          op: "is-empty",
          left: operand("requester", rule.requesterPath),
        },
        {
          op: "is-empty",
          left: operand("candidate", rule.candidatePath),
        },
      );
    }
    return alternatives.length === 1
      ? direct
      : { op: "any", filters: alternatives };
  });

  return SerializableFilterSchema.parse({
    op: "all",
    filters,
  });
}