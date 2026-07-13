# Git Access

This document is the operator-facing walkthrough for the hub's smart-HTTP git endpoints. It covers minting a token, configuring stock `git` so it presents that token to the hub, and the URL and `refPattern` grammars used by the two repo families the hub exposes: assets and agent state.

For the underlying authorization model — the relationship between session-authenticated grants and bearer-token `RepoActions` — see `docs/AUTH.md`. For the route reference, see `docs/ROUTES.md`.

## What the hub exposes over git

Two repo families speak the smart-HTTP wire:

- **Assets.** Operator-curated, tenant-scoped repositories spanning the `skill`, `agent-state`, `package-registry`, and `workflow` kinds. Both `git clone` (upload-pack) and `git push` (receive-pack) are supported.
- **Agent state.** Two URL grammars, both read-only over HTTP. The per-instance grammar exposes the runtime state of a single agent; the per-definition grammar exposes the deploy artifacts the hub materializes at instance launch.

Anonymous access is not supported. Every smart-HTTP request must present a hub-issued bearer token. Unauthenticated clones receive `401 Unauthorized` with a `WWW-Authenticate: Basic realm="Interchange"` challenge so stock git falls into its credential helper or askpass prompt.

## Minting a token

Tokens are minted through the REST API. A session cookie (from better-auth sign-in) authenticates the mint request itself.

### Personal access tokens (`itx_pat_*`)

Personal tokens are user-scoped. They optionally restrict to a single tenant.

```sh
curl -sS \
  -X POST "$HUB/api/me/git-tokens" \
  -H "Content-Type: application/json" \
  -b "$COOKIE_JAR" \
  -d '{
    "name": "laptop",
    "tenantId": "tnt_abc",
    "resource": "asset:*",
    "refPattern": "refs/heads/**",
    "actions": ["can_read", "can_push"],
    "expiresAt": "2026-12-31T00:00:00Z"
  }'
```

The response carries the plaintext secret exactly once:

```json
{
  "id": "gtk_...",
  "secret": "itx_pat_...",
  "name": "laptop",
  "kind": "pat",
  "claims": {
    "resource": "asset:*",
    "refPattern": "refs/heads/**",
    "actions": ["createPack", "resolveRef", "receivePack"],
    "expiresAt": "2026-12-31T00:00:00Z"
  }
}
```

The `claims` echo the token's resolved grant. `resource`, `refPattern`, and `expiresAt` pass through from the request unchanged, while the `can_read`/`can_push` action aliases are expanded into the underlying `RepoAction` set (`can_read` → `createPack`, `resolveRef`; `can_push` → `receivePack`).

The secret is not persisted in plaintext on the server — only its SHA-256 digest. There is no recovery flow if you lose the secret; mint a new token and revoke the old one.

### Service tokens (`itx_svc_*`)

Service tokens are tenant-bound at mint time. They identify the _user_ who minted them but operate in a specific tenant's principal context.

```sh
curl -sS \
  -X POST "$HUB/api/tenants/tnt_abc/git-tokens" \
  -H "Content-Type: application/json" \
  -b "$COOKIE_JAR" \
  -d '{
    "name": "ci-runner",
    "resource": "asset:def_xxx",
    "refPattern": "refs/heads/main",
    "actions": ["can_read"],
    "expiresAt": "2026-09-01T00:00:00Z"
  }'
```

### Revocation

Both kinds are soft-revoked with `DELETE /api/{me,tenants/:tenantId}/git-tokens/:tokenId`. Revocation is immediate: the next request bearing the revoked secret receives `403` with `code: "token_revoked"`.

## Pointing stock `git` at a token

The hub accepts both `Authorization: Bearer <secret>` and HTTP Basic with the secret in the password field (the username is logged but ignored). Stock git uses Basic, so the practical setup question is "how does git get the secret into the password slot."

Three approaches, in increasing order of robustness:

### 1. URL embedding (one-shot, debugging only)

```sh
git clone "https://x-access-token:$TOKEN@hub.example/api/tenants/tnt_abc/assets/skill/greet.git"
```

The token ends up in shell history and in `.git/config` if you don't strip the URL afterwards. Reserve for ad-hoc poking; do not use for anything that persists.

### 2. Credential helper

Stock git's credential helpers store the token persistently and replay it on every request to the matching host.

- **macOS** — use the system keychain helper:

  ```sh
  git config --global credential.helper osxkeychain
  ```

  On first request git prompts for username and password; supply any non-empty username (e.g. `x-access-token`) and the token as the password. Subsequent requests pull from the keychain.

- **Linux** — `libsecret` if available, otherwise `cache` or `store`:

  ```sh
  # Persistent via libsecret (preferred):
  git config --global credential.helper libsecret

  # In-memory for the session:
  git config --global credential.helper 'cache --timeout=3600'

  # File-backed (plaintext on disk; only on trusted machines):
  git config --global credential.helper store
  ```

- **Windows** — `manager` (Git Credential Manager) ships with Git for Windows:

  ```sh
  git config --global credential.helper manager
  ```

### 3. `GIT_ASKPASS` shim (scripted environments)

For CI and integration tests, the cleanest path is a `GIT_ASKPASS` script that echoes the token regardless of prompt. The integration harness uses this pattern — see `tokenAskpassEnv` in `tests/hub-api/lib/git-harness.ts`. Sketch:

```sh
cat >/tmp/askpass.sh <<'EOF'
#!/bin/sh
printf '%s\n' "$ITX_GIT_TOKEN"
EOF
chmod +x /tmp/askpass.sh
export GIT_ASKPASS=/tmp/askpass.sh
export GIT_TERMINAL_PROMPT=0
export ITX_GIT_TOKEN=itx_pat_...
git clone https://hub.example/api/tenants/tnt_abc/assets/skill/greet.git
```

`GIT_TERMINAL_PROMPT=0` prevents git from falling back to a TTY prompt if the askpass shim ever exits non-zero, which is what you want in CI — fail loud rather than block.

## URL grammar

### Assets

```
/api/tenants/:tenantId/assets/:kind/:name.git/...
```

- `:tenantId` is the `tnt_*` ID.
- `:kind` is one of `skill`, `agent-state`, `package-registry`, or `workflow`. (The `agent-state` _kind_ is the asset family used for shared agent-state templates; it is not the same surface as the per-instance and per-definition agent-state routes below.)
- `:name` is the kebab-case asset name as registered via `POST /api/tenants/:tenantId/assets`.
- The `.git` suffix on the trailing segment is required — it is how the hub disambiguates the asset namespace from arbitrary tenant sub-paths.

Worked examples:

```
# Clone the skill asset "greet" in tenant tnt_abc:
git clone https://hub.example/api/tenants/tnt_abc/assets/skill/greet.git

# Push to it:
cd greet
git push origin main
```

### Agent state — per instance

```
/api/tenants/:tenantId/agents/instances/:instanceId/state.git/...
```

- `:instanceId` is the `ins_*` ID.
- Read-only over HTTP. The instance's runtime state is written by the sidecar through a separate path; HTTP is for inspection.

```
git clone https://hub.example/api/tenants/tnt_abc/agents/instances/ins_xyz/state.git
```

### Agent state — per definition

```
/api/tenants/:tenantId/agents/definitions/:agentId/state.git/...
```

- `:agentId` is the `agt_*` ID.
- Read-only over HTTP. The `deploy/` prefix is populated by the hub at instance launch time.

```
git clone https://hub.example/api/tenants/tnt_abc/agents/definitions/agt_xyz/state.git
```

## `refPattern` grammar

The `refPattern` claim on a token restricts which refs the token may operate on. The matcher is a self-contained simple-glob — see `glob.match` in `@intx/hub-common`.

- A literal character matches itself.
- `*` matches any run of characters within a single `/`-delimited segment. It does not cross `/`.
- `**` matches any run of characters including `/`. It crosses segments and may match zero segments.

Worked examples:

| `refPattern`            | Matches                                      | Does not match            |
| ----------------------- | -------------------------------------------- | ------------------------- |
| `refs/heads/main`       | `refs/heads/main`                            | `refs/heads/feature/x`    |
| `refs/heads/*`          | `refs/heads/main`, `refs/heads/feature`      | `refs/heads/team/feature` |
| `refs/heads/**`         | `refs/heads/main`, `refs/heads/team/feature` | `refs/tags/v1`            |
| `refs/tags/v*`          | `refs/tags/v1`, `refs/tags/v2.0`             | `refs/tags/release-1`     |
| `**`                    | every ref                                    | (nothing)                 |
| `refs/heads/release/**` | `refs/heads/release/2026.06`                 | `refs/heads/main`         |

Pick the narrowest pattern that lets the token do its job. `refPattern: "**"` is appropriate for a developer's read-only laptop token against an asset they own; it is the wrong choice for a CI runner that only needs to push to one branch.

## `actions` claim

The token's `actions` claim names the `RepoAction`s the token is allowed to perform. Two user-facing aliases collapse the common cases:

- `can_read` — equivalent to `["createPack", "resolveRef"]`. Lets the token clone and fetch.
- `can_push` — equivalent to `["receivePack"]`. Lets the token push.

You can also enumerate the underlying `RepoAction`s directly (`init`, `writeTree`, `receivePack`, `createPack`, `resolveRef`). The mint endpoint expands aliases to their underlying actions at insert time, so the stored row always carries canonical names.

For an asset clone token: `actions: ["can_read"]`. For a CI runner that builds and pushes: `actions: ["can_read", "can_push"]`.

## Troubleshooting

### `Username for 'https://hub.example':` prompt

Git could not find a credential helper, your askpass shim is not on the PATH the git invocation sees, or the URL host does not match what the helper has cached. Fix the helper config, set `GIT_ASKPASS`, or — for one-off debugging — embed the token in the URL as shown above.

### `fatal: Authentication failed for ...`

Stock git's wording for a `401`. The hub returns `401` with `WWW-Authenticate: Basic realm="Interchange"` when:

- No `Authorization` header was sent.
- The header parses but the secret prefix is unrecognized (not `itx_pat_` or `itx_svc_`).
- The secret's SHA-256 digest does not match any row in `git_token`.

Re-mint the token if the secret is genuinely lost; otherwise check the credential helper is returning the value you think it is. `GIT_TRACE=1 GIT_CURL_VERBOSE=1 git ...` prints the outbound headers.

### `! [remote rejected]` on push

The hub returns `403` with one of the `git_token` error codes when the request authenticates but is not authorized:

- `code: "token_revoked"` — the token row's `revoked_at` is set. Mint a new token.
- `code: "token_expired"` — the token's `expires_at` is in the past. Mint a new one with a longer lifetime.
- `code: "tenant_mismatch"` — the URL's `:tenantId` does not match the token's `tenant_id` claim. Use the right URL or mint a token for the right tenant.
- `code: "principal_not_found"` — the token's principal no longer exists in the target tenant. The user was probably removed from the tenant.
- `code: "principal_suspended"` — the principal exists but its membership status is not `active`.

A `403` from the substrate authz layer (rather than the bearer middleware) carries the standard `forbidden` envelope and reflects that the token's `resource`/`refPattern`/`actions` claims do not cover the requested operation, or that the underlying principal does not hold a matching grant. Tokens and grants compose: both must allow the operation. See `docs/AUTH.md` for the full grant model.

### `401` vs `403` distinction

- **`401 Unauthorized`** — "I don't know who you are." Sent when the request carries no credential, or the credential is unparseable, or no token row matches.
- **`403 Forbidden`** — "I know who you are, and you're not allowed to do this." Sent when the token authenticates but its claims, or the underlying grants, do not cover the request.

`401` carries a `WWW-Authenticate` challenge header; `403` does not.

## Known limitations

- **Shallow clones are not supported.** `git clone --depth=N` triggers `protocol error: unexpected 'shallow' line` because the hub does not implement the smart-HTTP shallow negotiation. Track the follow-on issue for the implementation; clone full for now.
- **Anonymous access is not supported.** Every smart-HTTP request must present a bearer token. There is no public-read mode.
- **Pre-existing instances and definitions have no creator grants.** Instances and definitions created before the creator-grant seeding shipped do not carry an explicit creator grant on the asset or state repo. Operators working with that legacy data fall back to a tenant admin grant on the relevant resource. New instances and definitions seed the creator grant at creation and launch time, and need no special handling.

## Operational caveats

- **Packs are materialized in RAM.** `git.packObjects` (the substrate's pack producer) buffers the entire packfile in memory before streaming it to the client. Large repos amplify hub memory usage on every clone. Until a streaming implementation lands, keep an eye on RSS during fleet-wide clones, and avoid running a large bulk-clone job concurrently with peak control-plane load.
