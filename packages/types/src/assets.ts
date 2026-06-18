import { type } from "arktype";

const assetKindDescription =
  "Category of the asset, used together with `name` to address it. The (kind, name) pair is what callers resolve against, and it is unique within a tenant.";

export const AssetResponse = type({
  id: "string",
  tenantId: "string",
  kind: type("string").describe(assetKindDescription),
  name: "string",
  displayName: "string | null",
  creatorPrincipalId: "string | null",
  createdAt: "string",
  updatedAt: "string",
});

/**
 * `AssetResponse` extended with the tenant that supplied the row. The
 * inherited-list endpoint stamps every row with this tag so callers can
 * distinguish locally-defined assets from inherited ones without
 * issuing a second round-trip per row.
 */
export const AssetWithOriginResponse = type({
  id: "string",
  tenantId: "string",
  kind: type("string").describe(assetKindDescription),
  name: "string",
  displayName: "string | null",
  creatorPrincipalId: "string | null",
  createdAt: "string",
  updatedAt: "string",
  origin: type({
    tenantId: type("string").describe(
      "The tenant that supplied this row -- either the queried tenant itself or an ancestor it inherits from.",
    ),
    direct: type("boolean").describe(
      "True when the asset is declared on the queried tenant itself; false when it is inherited from an ancestor tenant.",
    ),
  }).describe(
    "Which tenant in the hierarchy this asset row came from, distinguishing locally-defined assets from inherited ones.",
  ),
});
