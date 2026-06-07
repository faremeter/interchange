import { type } from "arktype";

export const AssetResponse = type({
  id: "string",
  tenantId: "string",
  kind: "string",
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
  kind: "string",
  name: "string",
  displayName: "string | null",
  creatorPrincipalId: "string | null",
  createdAt: "string",
  updatedAt: "string",
  origin: {
    tenantId: "string",
    direct: "boolean",
  },
});
