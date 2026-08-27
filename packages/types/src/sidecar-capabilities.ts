import { type } from "arktype";

export type ParsedSidecarCapabilitySelector = {
  readonly kind: "exact" | "prefix";
  readonly segments: readonly string[];
};

export function parseSidecarCapabilitySelector(
  value: string,
): ParsedSidecarCapabilitySelector | null {
  if (value.length === 0) return null;
  if (value === "*") return { kind: "prefix", segments: [] };
  if (!value.includes("*")) {
    const segments = value.split(":");
    return segments.some((segment) => segment.length === 0)
      ? null
      : { kind: "exact", segments };
  }

  if (!value.endsWith(":*") || value.indexOf("*") !== value.length - 1) {
    return null;
  }
  const segments = value.slice(0, -2).split(":");
  if (segments.some((segment) => segment.length === 0)) return null;
  return {
    kind: "prefix",
    segments,
  };
}

export const SidecarCapabilitySelector = type("string > 0").narrow(
  (value, ctx) =>
    parseSidecarCapabilitySelector(value) !== null ||
    ctx.mustBe(
      "an exact capability, a trailing namespace selector such as runtime:*, or *",
    ),
);
export type SidecarCapabilitySelector = typeof SidecarCapabilitySelector.infer;

export const SidecarCapabilityRule = type({
  capability: SidecarCapabilitySelector,
  effect: "'require' | 'block'",
});
export type SidecarCapabilityRule = typeof SidecarCapabilityRule.infer;

export const SidecarCapabilityDeclaration = type({
  capability: SidecarCapabilitySelector,
  state: "'available' | 'blocked'",
});
export type SidecarCapabilityDeclaration =
  typeof SidecarCapabilityDeclaration.infer;

export const SidecarCapabilityPolicy = type({
  "capabilities?": SidecarCapabilityRule.array(),
}).onUndeclaredKey("reject");
export type SidecarCapabilityPolicy = typeof SidecarCapabilityPolicy.infer;

export type TenantSidecarCapabilityPolicy = {
  readonly tenantId: string;
  readonly rules: readonly SidecarCapabilityRule[];
};
