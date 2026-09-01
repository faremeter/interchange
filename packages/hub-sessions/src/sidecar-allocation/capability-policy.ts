import {
  parseSidecarCapabilitySelector,
  type ParsedSidecarCapabilitySelector,
  type SidecarCapabilityDeclaration,
  type SidecarCapabilityRule,
  type TenantSidecarCapabilityPolicy,
} from "@intx/types";

export type EffectiveSidecarCapabilityPolicy = {
  readonly tenantPolicies: readonly TenantSidecarCapabilityPolicy[];
  readonly probeRules?: readonly SidecarCapabilityRule[];
  readonly workflowRules: readonly SidecarCapabilityRule[];
};

export type SidecarCapabilityMismatch = {
  readonly capability: string;
  readonly expected: "available" | "blocked";
  readonly actual: "available" | "blocked" | "unknown";
  readonly rule: SidecarCapabilityRule;
  readonly source:
    | { readonly kind: "tenant"; readonly tenantId: string }
    | { readonly kind: "probe" }
    | { readonly kind: "workflow" };
};

export type SidecarCapabilityMatch =
  | { readonly ok: true }
  | {
      readonly ok: false;
      readonly mismatches: readonly SidecarCapabilityMismatch[];
    };

type ResolvedRule = {
  readonly rule: SidecarCapabilityRule;
  readonly selector: ParsedSidecarCapabilitySelector;
  readonly specificity: number;
};

type ResolvedDeclaration = {
  readonly declaration: SidecarCapabilityDeclaration;
  readonly selector: ParsedSidecarCapabilitySelector;
  readonly specificity: number;
};

type SelectorBoundary = ParsedSidecarCapabilitySelector;

export function matchSidecarCapabilityPolicy(
  policy: EffectiveSidecarCapabilityPolicy,
  declarations: readonly SidecarCapabilityDeclaration[],
): SidecarCapabilityMatch {
  const tenantPolicies = policy.tenantPolicies.map((tenantPolicy) => ({
    ...tenantPolicy,
    rules: tenantPolicy.rules.map(parseRule),
  }));
  const probeRules = (policy.probeRules ?? []).map(parseRule);
  const workflowRules = policy.workflowRules.map(parseRule);
  const parsedDeclarations = declarations.map(parseDeclaration);
  const boundaries = collectBoundaries(
    tenantPolicies.flatMap(({ rules }) =>
      rules.map(({ selector }) => selector),
    ),
    probeRules.map(({ selector }) => selector),
    workflowRules.map(({ selector }) => selector),
    parsedDeclarations.map(({ selector }) => selector),
  );

  const mismatches: SidecarCapabilityMismatch[] = [];
  for (const boundary of boundaries) {
    const capability = formatSelector(boundary);
    for (const tenantPolicy of tenantPolicies) {
      const resolved = resolveRules(tenantPolicy.rules, boundary);
      if (resolved !== null) {
        addMismatch(
          mismatches,
          resolved.rule,
          resolveDeclarations(parsedDeclarations, boundary),
          capability,
          { kind: "tenant", tenantId: tenantPolicy.tenantId },
        );
      }
    }

    const probeRule = resolveRules(probeRules, boundary);
    if (probeRule !== null) {
      addMismatch(
        mismatches,
        probeRule.rule,
        resolveDeclarations(parsedDeclarations, boundary),
        capability,
        { kind: "probe" },
      );
    }

    const workflowRule = resolveRules(workflowRules, boundary);
    if (workflowRule !== null) {
      addMismatch(
        mismatches,
        workflowRule.rule,
        resolveDeclarations(parsedDeclarations, boundary),
        capability,
        { kind: "workflow" },
      );
    }
  }

  return mismatches.length === 0 ? { ok: true } : { ok: false, mismatches };
}

function addMismatch(
  mismatches: SidecarCapabilityMismatch[],
  rule: SidecarCapabilityRule,
  declaration: ResolvedDeclaration | null,
  capability: string,
  source: SidecarCapabilityMismatch["source"],
): void {
  const expected = rule.effect === "require" ? "available" : "blocked";
  const actual = declaration?.declaration.state ?? "unknown";
  if (actual === expected) return;
  mismatches.push({ capability, expected, actual, rule, source });
}

function resolveRules(
  rules: readonly ResolvedRule[],
  boundary: SelectorBoundary,
): ResolvedRule | null {
  let winner: ResolvedRule | null = null;
  for (const candidate of rules) {
    if (!selectorAppliesToBoundary(candidate.selector, boundary)) continue;
    if (
      winner === null ||
      candidate.specificity > winner.specificity ||
      (candidate.specificity === winner.specificity &&
        candidate.rule.effect === "block" &&
        winner.rule.effect === "require")
    ) {
      winner = candidate;
    }
  }
  return winner;
}

function resolveDeclarations(
  declarations: readonly ResolvedDeclaration[],
  boundary: SelectorBoundary,
): ResolvedDeclaration | null {
  let winner: ResolvedDeclaration | null = null;
  for (const candidate of declarations) {
    if (!selectorAppliesToBoundary(candidate.selector, boundary)) continue;
    if (
      winner === null ||
      candidate.specificity > winner.specificity ||
      (candidate.specificity === winner.specificity &&
        candidate.declaration.state === "blocked" &&
        winner.declaration.state === "available")
    ) {
      winner = candidate;
    }
  }
  return winner;
}

function parseRule(rule: SidecarCapabilityRule): ResolvedRule {
  const selector = parseSelector(rule.capability);
  return {
    rule,
    selector,
    specificity: selector.segments.length,
  };
}

function parseDeclaration(
  declaration: SidecarCapabilityDeclaration,
): ResolvedDeclaration {
  const selector = parseSelector(declaration.capability);
  return {
    declaration,
    selector,
    specificity: selector.segments.length,
  };
}

function parseSelector(capability: string): ParsedSidecarCapabilitySelector {
  const selector = parseSidecarCapabilitySelector(capability);
  if (selector === null) {
    throw new Error(
      `Invalid sidecar capability selector: ${JSON.stringify(capability)}`,
    );
  }
  return selector;
}

function collectBoundaries(
  ...selectorGroups: readonly (readonly ParsedSidecarCapabilitySelector[])[]
): readonly SelectorBoundary[] {
  const boundaries = new Map<string, SelectorBoundary>();
  for (const selectors of selectorGroups) {
    for (const selector of selectors) {
      boundaries.set(`${selector.kind}:${formatSelector(selector)}`, selector);
    }
  }
  return [...boundaries.values()];
}

function selectorAppliesToBoundary(
  selector: ParsedSidecarCapabilitySelector,
  boundary: SelectorBoundary,
): boolean {
  if (selector.kind === "exact") {
    return (
      boundary.kind === "exact" &&
      segmentsEqual(selector.segments, boundary.segments)
    );
  }
  if (!isSegmentPrefix(selector.segments, boundary.segments)) return false;
  return (
    boundary.kind === "prefix" ||
    selector.segments.length < boundary.segments.length
  );
}

function isSegmentPrefix(
  prefix: readonly string[],
  segments: readonly string[],
): boolean {
  return (
    prefix.length <= segments.length &&
    prefix.every((segment, index) => segment === segments[index])
  );
}

function segmentsEqual(
  left: readonly string[],
  right: readonly string[],
): boolean {
  return (
    left.length === right.length &&
    left.every((segment, index) => segment === right[index])
  );
}

function formatSelector(selector: ParsedSidecarCapabilitySelector): string {
  if (selector.kind === "exact") return selector.segments.join(":");
  if (selector.segments.length === 0) return "*";
  return `${selector.segments.join(":")}:*`;
}
