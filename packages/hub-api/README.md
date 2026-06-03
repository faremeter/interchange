# @intx/hub-api

Hono application factory for the hub. Owns the public HTTP API
surface: routes, middleware (session, tenant resolution, grant
enforcement), authentication via better-auth, and the OpenAPI
spec served from the running app.

Consumed by `apps/hub` as the HTTP entry point. The factory takes
the database client, session service, sidecar router, repo store,
asset service, and event collector registry from `@intx/db` and
`@intx/hub-sessions` and returns a configured Hono app.

`createApp` returns a configured Hono app. Its `CreateAppOpts`
parameter wires together the dependencies the API needs: a
better-auth `authHandler` and matching `getSession`, the database
client (`db`), the `SidecarRouter` and `SessionService` from
`@intx/hub-sessions`, and an `EventCollectorRegistry`. The
`assetService` and `repoStore` fields must be supplied but accept
`null` for deployments that don't host the git surface; the
`grantStore` and `sidecarWsHandler` fields are fully optional. See
`CreateAppOpts` in `src/app.ts` for the exact field list. The
companion `createAuth` factory builds the better-auth instance
that supplies `authHandler` and `getSession`.

`createRequireGrant` is the grant-enforcement middleware factory
exposed for callers that mount additional routes against the same
authorization stack; it composes with the tenant and session
middleware so every route sees a resolved principal, tenant, and
grant decision.
