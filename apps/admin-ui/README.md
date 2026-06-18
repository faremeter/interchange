# @intx/admin-ui

The browser single-page application operators use to manage the
control plane. A React 19 app built and served with Vite, talking
to the hub API through `@intx/hub-client`.

`src/main.tsx` mounts the React root, installs a TanStack Query
client, and renders the TanStack Router defined in `src/router.tsx`.
The router gates every authenticated route behind a `/api/me`
session check (redirecting to `/login` on failure) and exposes the
tenant-management console: a dashboard plus per-tenant pages for
agents, instances, principals, roles, grants, credentials, wallets,
and offerings, each with a list and detail view.

Develop with `bun run dev` (Vite dev server), build with
`bun run build`, and preview a production build with
`bun run preview`. The dev server proxies `/api` to a running
`apps/hub`.
