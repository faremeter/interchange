// Hub-owned session contract.
//
// SessionUser and SessionInfo mirror the structural shape that better-auth
// currently returns (see @better-auth/core/dist/db/schema/{user,session}),
// but the hub does not reference Auth["$Infer"]. This breaks the type-level
// dependency on better-auth so a third-party identity provider can be
// plugged in by satisfying the GetSession contract.
//
// The shapes are kept intentionally hand-written: deriving with Pick<>
// against the better-auth types would re-introduce the type dependency.
// The trade-off is that a field added upstream in better-auth would not
// surface here automatically.
//
// The optional `?: T | null | undefined` shape (rather than `?: T | null`)
// is deliberate. Under exactOptionalPropertyTypes, the latter rejects an
// explicit `undefined` assignment, but the inferred return of
// z.string().nullish() in better-auth's user/session schemas is
// `string | null | undefined`. Without the trailing `| undefined`, the
// adapter in apps/hub cannot pass the result through structurally.

export type SessionUser = {
  id: string;
  createdAt: Date;
  updatedAt: Date;
  email: string;
  emailVerified: boolean;
  name: string;
  image?: string | null | undefined;
};

export type SessionInfo = {
  id: string;
  createdAt: Date;
  updatedAt: Date;
  userId: string;
  expiresAt: Date;
  token: string;
  ipAddress?: string | null | undefined;
  userAgent?: string | null | undefined;
};

export type GetSession = (
  headers: Headers,
) => Promise<{ user: SessionUser; session: SessionInfo } | null>;
