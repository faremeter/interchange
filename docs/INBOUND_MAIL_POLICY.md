# Inbound Mail Admission Policy

Every workflow run has an address, so a run of any trigger can receive mail
from outside itself. The hub stamps each inbound message with its
authenticated sender, and co-delivers the public key it vouches for that
sender. The recipient's own sidecar verifies the signature locally at the
delivery boundary, against the key it cached from that co-delivery, and then
decides per message whether the recipient workflow gets it. `inboundMailPolicy`
is how a workflow author takes part in that decision.

Every outcome the author controls defaults to `reject`. A workflow that declares
no policy therefore receives only mail whose signature verified against the key
held for the sender the hub stamped, and whose visible `From` binds to that same
sender. Everything else is dropped before the workflow sees it: no delivery, no
reply frame, no acknowledgement, no error back to the sender. A workflow that is
not receiving mail an author expected is the symptom this document exists for.

The signature gate is the second of two gates. Before it, the hub admits or
refuses the sender itself against the workflow's `mailAccept` rules. A
workflow that declares no `mailAccept` admits a sender only through an
operator grant on the run principal, so it refuses the first mail that fires
it. See _The sender gate runs first_ below.

## Declaring a policy

```ts
const workflow = defineWorkflow({
  id: "support-intake",
  agent: intakeAgent,
  trigger: { type: "mail", to: "support@acme.example" },
  mailAccept: { tenant: true },
  inboundMailPolicy: {
    unknown: "admit",
  },
});
```

The `mailAccept` rule lets the sender gate admit senders in the workflow's own
tenant. When no `mailAccept` rule and no operator grant admits a sender, the
sender gate refuses the mail before the signature gate runs, and
`inboundMailPolicy` has no effect on it.

The field is valid only on a workflow that has a mail trigger; `defineWorkflow`
throws otherwise. A mail trigger contributed by an `onTrigger` section counts.
A workflow with another trigger can still receive mail, for example the replies
that a `correspondent` rule admits. The signature gate applies the default
policy to that mail, and the author cannot change it.

The object is sparse. An omitted key is not written out as a default, so a
workflow that declares no policy hashes identically to one authored before the
field existed. Adding a key moves the deployment's content hash.

## The keys

Each key names one condition the gate can find, and its value decides whether a
message raising that condition is `reject`ed or `admit`ted. The default for
every key is `reject`. A key outside the six, or a value outside
`"reject" | "admit"`, is refused twice: by TypeScript where the workflow is
authored, and by the wire schema when the deployment reaches the sidecar.

| Key              | The condition it names                                                                                                                                                                                                                                                                                                            | What relaxing it accepts                                                                                                                                                                                             |
| ---------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `unknown`        | No key was available to verify against: the local cache was never given a key for the sender the hub stamped. The signature is never examined on this path, so an unsigned message from an uncached sender is `unknown`, not `missing`. A sender the cache does hold an entry for, but whose entry could not be read, is `error`. | A message whose originator identity is not cryptographically established at all. A sender who can get the hub to stamp an address of their choosing, and who puts that same address in the `From`, is admitted here. |
| `missing`        | The message carried no signature: it is not `multipart/signed`, it declares no `boundary=` parameter, or it carries no `application/pgp-signature` part.                                                                                                                                                                          | Mail from a correspondent who does not sign at all. The originator identity is unverified, as with `unknown`.                                                                                                        |
| `invalid`        | The signature check failed: tampering, the wrong key, or a message that claims to be signed and cannot be parsed as one.                                                                                                                                                                                                          | The one author-controllable outcome with no benign reading. Relax it only with a stated reason.                                                                                                                      |
| `untrustedFrom`  | The visible `From` is present and cannot be reduced to one address.                                                                                                                                                                                                                                                               | A message the gate could make no originator claim about at all.                                                                                                                                                      |
| `mismatchedFrom` | The visible `From` names a different address from the sender the hub stamped, so the message contradicts itself about who sent it. Raised under every non-error signature status.                                                                                                                                                 | A message delivered as though it came from an address its own transport does not agree with. Under a verified signature this is an identity forgery — a legitimate key signing under a borrowed display identity.    |
| `absentFrom`     | The message carries no usable `From`, so the gate resolved no originator. Raised under every non-error signature status.                                                                                                                                                                                                          | Mail that names no originator. The gate holds the hub's stamp, but nothing downstream reads it.                                                                                                                      |

For mail that the hub relays to a run, the sender gate runs first and refuses a
sender that the hub cannot resolve to a principal. So on that path, relaxing
`unknown` or `missing` does not admit a correspondent outside Interchange. It
changes only what the signature gate does with mail from a sender that the
sender gate admitted.

## Two outcomes are not keys

- `clean` is pinned to `admit`. Nothing about the message was suspect — a
  signature that verified, over a message whose visible `From` binds to the
  sender the hub stamped. The signature gate of a live deployment admits it
  whatever its author declared in `inboundMailPolicy`. The sender gate still
  applies, and an address with no live deployment is the exception; see below.
- `error` is pinned to `reject`. A fault stopped the check from running at all,
  so there is no trust claim to make: the hub's sender stamp was unreadable, or
  the local key cache refused to answer for the sender because that sender's
  cached entry failed to load. `error` is not a key in `InboundMailPolicy`, and
  the decision short-circuits before it reads the policy, so nothing an author
  writes can wave it past. A key that is usable but is not the signer's is not a
  fault — the check ran and failed, which is the author-controllable `invalid`.

A cached entry that failed to load is an operator condition, never a sender one,
and the gate keeps it one. A truncated write, a corrupt file, or an envelope
whose address disagrees with its filename makes the cache refuse that address
rather than report it as a sender it holds no key for, so the outcome is `error`
and not the author-controllable `unknown`. A workflow author therefore has
nothing to do about it: relaxing `unknown` does not admit the mail.

Two ERROR lines name the condition. The cache logs the file, the sender, and the
reason when it loads the keyring at start-up. The gate then logs the fault,
naming the sender and the reason, in place of the usual verdict line, each time
mail from that sender arrives. The repair is driven from the hub: the sidecar
reports the affected sender on its next reconnect, and the hub re-resolves it —
re-pushing the current key, which replaces the unreadable file, or evicting the
sender if its principal is gone, which deletes the file. A hub-side resolve that
itself faults changes nothing, so the entry stays unreadable until a later
reconnect. A run sender is repaired by the next grants barrier that co-delivers
its key instead, because the hub never re-resolves a run sender on reconnect: a
run's key is immutable.

## Admission weighs every finding, not one

Verification has two independent axes: whether the signature verified, and what
the gate observed about the message's visible `From`. One message can raise a
complaint on both at once. Admission collects every complaint the message raised
and admits the message only if the policy admits all of them.

Relax one key and you relax exactly that one condition, not a class of message.
A message from an uncached sender that carries no `From` raises `unknown` and
`absentFrom`, so relaxing `unknown` alone still drops it. The rule holds in the
other direction, which is what it is for: a malformed `From` alongside a failed
signature raises `untrustedFrom` and `invalid` together, so relaxing
`untrustedFrom` cannot switch the signature check off.

## The policy belongs to a live deployment

The sidecar resolves the authored sparse policy into a total decision map once,
when it spawns the deployment, and stores it against the deployment's mail
address. Teardown removes it, so a reused address never inherits a stale policy.

An address the store does not hold gets a fully-closed policy, in which every
outcome rejects — `clean` included. An address that never had a deployment
hydrated behind it, or whose deployment was already torn down, has no author
intent to honor, so it admits nothing. This is stricter than an authored policy
that declares nothing: a live deployment whose author declared no policy still
receives `clean` mail from a sender the sender gate admits.

## The sender gate runs first

The hub decides whether a sender may reach a deployment before it relays the
mail. The decision uses the workflow's `mailAccept` rules, not
`inboundMailPolicy`, and it identifies the sender by its authenticated address,
never by the visible `From`. The gate covers mail to every run, whatever its
trigger, and a workflow with no mail trigger declares `mailAccept` the same
way.

```ts
const workflow = defineWorkflow({
  id: "ops-requests",
  agent: opsAgent,
  trigger: { type: "mail", to: "ops@acme.example" },
  mailAccept: { tenant: true, correspondent: true },
});
```

This workflow takes requests from principals in its own tenant, and it admits
the reply from each party that it mails.

Each relation admits a sender that stands in that relation to the deployment:

- `invoker` admits the deployment's deployer, its invoker of record: the
  principal that deployed it, a user or another run (see "Grant Requirements
  on Definitions" in AUTH.md). It names the same principal whoever triggers
  the run, so it admits no other sender. A deployment that another run
  deployed admits mail from that run.
- `self` admits a deployment of any definition that the same workflow asset
  backs.
- `tenant` admits any principal in the deployment's tenant.
- `correspondent` admits the reply from a party the deployment mailed. A
  workflow that sends mail and waits for the reply declares it, whatever its
  trigger.
- `principals` and `assets` name explicit senders by literal id. An id may not
  contain `:` or `*`.

The default is deny. A workflow that declares no `mailAccept`, or declares no
rule that matches the sender, refuses the mail unless an operator grant on the
run principal admits the sender. The first mail that fires a run has no run
principal yet, so no operator grant can admit it. An unknown sender is always
refused. The operator approves the rules at deploy time, as the `mail.accept:`
strings of the capability walk. `defineWorkflow` rejects a grant requirement in
the `mail.accept` namespace, so `mailAccept` is the only way to author a rule.

No `mailAccept` rule admits an arbitrary outside sender: `principals` and
`assets` name literal ids, and `invoker` admits only the deployer. A
mail-trigger address that takes mail from many outside senders, such as a
public support address, needs an operator `mail.accept` grant on the run
principal, and that grant can exist only after the run exists. So the run's
first mail must come from a sender a rule admits: the deployer, a principal of
the run's tenant under `tenant`, or a sender `principals` or `assets` names.

At launch, each rule except `correspondent` becomes a
`mail.accept:<coord-type>:<id>` grant on the run principal. A `correspondent`
grant for a party is added when the run mails that party. The hub resolves the
sender to its coordinates: its principal id, the asset behind its definition
(for a deployment whose definition has an asset), and its tenant id. The first
mail that fires a run is checked against the run's staged grants before the run
exists. Later mail is checked against the grants that `collectGrants` returns
for the run principal: its direct grants and the grants of its roles in the
run's tenant. An operator can therefore refuse a sender with a `deny` on the
run principal or one of its roles. The deny must use the action `accept`, the
action of the launch and `correspondent` grants. Specificity counts the action,
so a `deny` with the action `*` ranks below an `accept` allow at the same
coordinate and does not override it.

The grants that match any of the sender's coordinates form one set, and one
grant wins. The order is:

1. The coordinate the grant names: `principal`, then `asset`, then `tenant`,
   then a namespace-wide grant such as `mail.accept:*`.
2. Pattern specificity.
3. Effect: `deny`, then `ask`, then `allow`.

The coordinate type ranks first, before pattern specificity. An
`allow mail.accept:principal:<id>` admits that sender past a
`deny mail.accept:*` or a `deny mail.accept:tenant:<id>`. A wildcard id does
not lower the rank: an `allow mail.accept:principal:*` also outranks an exact
`deny mail.accept:tenant:<id>`. The rank comes from the literal type segment,
so a grant with a wildcard in that segment, such as `mail.accept:principal*`,
names no coordinate and ranks as namespace-wide. A winning `ask`
refuses the mail, because a delivery cannot wait for a prompt. Only grants in
the `mail.accept` namespace take part, so a broad `*` or `mail.*` grant never
admits mail.

A namespace-wide grant ranks below every coordinate, so a `deny mail.accept:*`
does not close a run to mail: each launch grant names a coordinate and outranks
it. An exact `deny` with the action `accept` on a coordinate that the run's
grants name refuses that coordinate, for example
`deny mail.accept:principal:<id>` for the invoker and
`deny mail.accept:tenant:<id>` for the tenant rule.

A run with the `correspondent` rule gets a new exact
`allow mail.accept:principal:<party>` for each new party it mails. That allow
outranks every broader deny: `deny mail.accept:*`,
`deny mail.accept:tenant:<id>` and `deny mail.accept:principal:*`. So a broad
deny does not close the run to the parties it mails later. An exact
`deny mail.accept:principal:<party>` with the action `accept` refuses that
party, whether the operator writes it before or after the run mails the party.
To close the run to every party, end the run.

### State that predates the sender gate

Deployments and runs that exist before the sender gate have no `mail.accept`
rules. The migration `0113_mail_accept_backfill` gives them rules, so they keep
receiving mail. It classifies each deployment by the grant snapshot in its
launch spec, the snapshot the hub reads at runtime, and each run by the launch
spec of its deployment:

- Each live deployment of a definition with a mail trigger gets the legacy rule
  `invoker-correspondent` on its anchor run, and its runs get a
  `correspondent` grant for each party they mail. The `invoker` half helps
  only a deployment that has not fired: its first fire admits its deployer. A
  deployment that already fired keeps its run, so from now on it admits only
  senders in its own tenant and the parties it mails. To admit other outside
  senders, it needs an operator `mail.accept` grant on the run principal.
- Each live deployment of a definition with no mail trigger whose agent can
  send mail (its snapshot grants `tool:mail_send`) gets the legacy rule
  `correspondent`. Its runs get a `correspondent` grant for each party they
  mail.
- Each live run that has a run principal, of a definition with a mail trigger
  or with any mail tool (`mail_send`, `mail_reply`, `mail_wait`, `mail_read`,
  `mail_search`, `mail_flag` or `mail_expunge`), gets a
  `mail.accept:tenant:<id>` grant for its own tenant. The backfill adds no
  invoker row and does not know the parties it mailed, so an existing run
  admits only senders in its own tenant.
- A deployment that has not fired has no run principal, so it gets only the
  legacy rule and no tenant grant. After its first fire, a mail-trigger
  deployment admits its invoker and the parties it mails. A sending deployment
  admits only the parties it mails, so mail from the HTTP caller that started
  the run is refused. Neither admits other senders in its own tenant, so each
  admits fewer senders than a deployment that fired before the migration.
- A known gap remains. A live run that mailed a party outside its tenant before
  the migration has no `correspondent` grant for that party, so the reply from
  that party is refused.
- A deployment of a definition with a mail tool but no mail trigger and no
  `mail_send` gets no legacy rule, so a run it starts from now on is the same
  as a run of a new deployment.
- A definition with no mail trigger and no mail tool reads no mail before the
  gate either. It gets no rule, and its deployments and runs get no grant.

A legacy rule belongs to one deployment that existed before the gate. It is
stored on the deployment's anchor run (`workflow_run.legacy_mail_accept`), and
the hub adds its markers to the markers of the deployment's launch-spec
snapshot at a mail-triggered first fire, at a launch through the HTTP trigger
route, and at the correspondent mint. The migration does not change the
launch-spec snapshot, which holds the grant shape the operator approved. A new
deployment of the definition gets no legacy rule: it takes its rules from its
approved `mailAccept` alone, so with no `mailAccept` it admits no sender.

## Where this lives

- `InboundMailOutcome`, `AuthorControllableOutcome` and `InboundMailPolicy`,
  `packages/types/src/inbound-mail-policy.ts`
- The definition-time rule: `normalize`,
  `packages/workflow/src/definition/workflow.ts`
- The sender gate: `MailAccept`, `packages/types/src/mail-accept.ts`;
  `evaluateMailAdmission`, `packages/authz/src/mail-admission.ts`; and the
  materializer that applies it, `createMailTriggeredRunGrantsMaterializer`,
  `packages/hub-api/src/run-grant-materialization.ts`
- The defaults and the decision: `resolveInboundMailPolicy` and
  `decideInboundAdmission`, `packages/hub-agent/src/ws/inbound-signature.ts`.
  The decision tables in `inbound-signature.test.ts` beside them are the
  authoritative statement of the behaviour.
- The signature check itself: `verifyMimeSignature`,
  `packages/mailbox/src/verify-signature.ts`
- The keyring the check verifies against, and its load-fault handling:
  `createSenderKeyCache`, `packages/hub-agent/src/sender-key-cache.ts`
- The fully-closed default: `FULLY_CLOSED_INBOUND_MAIL_POLICY` and
  `createInboundMailPolicyLookup`,
  `packages/hub-agent/src/ws/inbound-mail-policy-registry.ts`
- Enforcement: the `mail.inbound` case of
  `packages/hub-agent/src/ws/hub-link.ts`
- Per-deployment resolution and registration:
  `packages/workflow-host/src/deploy/workflow-host-wiring.ts`
