# Inbound Mail Admission Policy

A workflow with a mail trigger receives mail from outside itself. The hub stamps
each inbound message with its authenticated sender, and co-delivers the public
key it vouches for that sender. The recipient's own sidecar verifies the
signature locally at the delivery boundary, against the key it cached from that
co-delivery, and then decides per message whether the recipient workflow gets
it. `inboundMailPolicy` is how a workflow author takes part in that decision.

Every outcome the author controls defaults to `reject`. A workflow that declares
no policy therefore receives only mail whose signature verified against the key
held for the sender the hub stamped, and whose visible `From` binds to that same
sender. Everything else is dropped before the workflow sees it: no delivery, no
reply frame, no acknowledgement, no error back to the sender. A workflow that is
not receiving mail an author expected is the symptom this document exists for.

## Declaring a policy

```ts
const workflow = defineWorkflow({
  id: "support-intake",
  agent: intakeAgent,
  trigger: { type: "mail", to: "support@acme.example" },
  inboundMailPolicy: {
    unknown: "admit",
  },
});
```

The field is valid only on a workflow that has a mail trigger; `defineWorkflow`
throws otherwise, because a policy no mail can reach is a silent authoring
error. A mail trigger contributed by an `onTrigger` section counts.

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
| `missing`        | The message carried no signature: it is not `multipart/signed`, it declares no `boundary=` parameter, or it carries no `application/pgp-signature` part.                                                                                                                                                                          | Mail from a correspondent who does not sign at all — the usual key for correspondents outside Interchange. The originator identity is unverified, as with `unknown`.                                                 |
| `invalid`        | The signature check failed: tampering, the wrong key, or a message that claims to be signed and cannot be parsed as one.                                                                                                                                                                                                          | The one author-controllable outcome with no benign reading. Relax it only with a stated reason.                                                                                                                      |
| `untrustedFrom`  | The visible `From` is present and cannot be reduced to one address.                                                                                                                                                                                                                                                               | A message the gate could make no originator claim about at all.                                                                                                                                                      |
| `mismatchedFrom` | The visible `From` names a different address from the sender the hub stamped, so the message contradicts itself about who sent it. Raised under every non-error signature status.                                                                                                                                                 | A message delivered as though it came from an address its own transport does not agree with. Under a verified signature this is an identity forgery — a legitimate key signing under a borrowed display identity.    |
| `absentFrom`     | The message carries no usable `From`, so the gate resolved no originator. Raised under every non-error signature status.                                                                                                                                                                                                          | Mail that names no originator. The gate holds the hub's stamp, but nothing downstream reads it.                                                                                                                      |

## Two outcomes are not keys

- `clean` is pinned to `admit`. Nothing about the message was suspect — a
  signature that verified, over a message whose visible `From` binds to the
  sender the hub stamped. A live deployment admits it whatever its author
  declared. An address with no live deployment is the exception; see below.
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
receives `clean` mail.

## Where this lives

- `InboundMailOutcome`, `AuthorControllableOutcome` and `InboundMailPolicy`,
  `packages/types/src/runtime.ts`
- The definition-time rule: `normalize`,
  `packages/workflow/src/definition/workflow.ts`
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
