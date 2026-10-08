import { type } from "arktype";

/**
 * The admission outcome of an inbound message. This is the single vocabulary a
 * delivery decision keys on, distinct from the two-axis signature verdict that
 * produces it.
 *
 * - `clean` — nothing suspect; always admitted
 * - `untrustedFrom` — the visible `From` is present but cannot be reduced to a
 *   single addr-spec
 * - `mismatchedFrom` — the visible `From` names a different identity from the
 *   sender the hub stamped
 * - `absentFrom` — the message carries no usable `From`
 * - `invalid` — the signature check failed (tampering or the wrong key)
 * - `missing` — the message carried no signature
 * - `unknown` — no key was available to verify against
 * - `error` — a fault stopped the check from running at all; always rejected
 */
export const InboundMailOutcome = type.enumerated(
  "clean",
  "untrustedFrom",
  "mismatchedFrom",
  "absentFrom",
  "invalid",
  "missing",
  "unknown",
  "error",
);
export type InboundMailOutcome = typeof InboundMailOutcome.infer;

/**
 * The subset of {@link InboundMailOutcome} a workflow author may relax to admit
 * a message that would otherwise be rejected. It omits `clean` (which always
 * admits, so there is nothing to relax) and `error` (pinned to reject, since a
 * fault we could not check through is never something an author should be able
 * to wave past). A per-workflow policy keys on exactly these outcomes.
 */
export const AuthorControllableOutcome = type.enumerated(
  "untrustedFrom",
  "mismatchedFrom",
  "absentFrom",
  "invalid",
  "missing",
  "unknown",
);
export type AuthorControllableOutcome = typeof AuthorControllableOutcome.infer;

/**
 * A per-workflow inbound-mail admission policy: for each admission outcome the
 * author may control, whether a message that raises that outcome as a finding is
 * `reject`ed or `admit`ted. The key set is exactly the
 * {@link AuthorControllableOutcome} values -- `clean` (always admitted) and
 * `error` (pinned to reject) are deliberately not keys.
 *
 * The object is SPARSE: every key is optional, and an omitted key is NOT a
 * default of any kind here. It is left for a later resolution step to interpret
 * an absent outcome. Keeping it sparse means the content hash covers only what
 * the author actually declared, so a definition that omits the policy hashes
 * identically to one authored before the field existed. Undeclared keys are
 * rejected so a typo such as `clean` or `errror` fails at the wire boundary
 * rather than riding through as an inert unknown key.
 */
export const InboundMailPolicy = type({
  "untrustedFrom?": "'reject' | 'admit'",
  "mismatchedFrom?": "'reject' | 'admit'",
  "absentFrom?": "'reject' | 'admit'",
  "invalid?": "'reject' | 'admit'",
  "missing?": "'reject' | 'admit'",
  "unknown?": "'reject' | 'admit'",
}).onUndeclaredKey("reject");
export type InboundMailPolicy = typeof InboundMailPolicy.infer;

type AssertEqual<A, B> = [A] extends [B]
  ? [B] extends [A]
    ? true
    : false
  : false;

/**
 * The three outcome vocabularies must move together, but nothing else connects
 * them: each spells the same names out again in its own notation.
 */
const _policyKeysMatchAuthorControllable: AssertEqual<
  keyof InboundMailPolicy,
  AuthorControllableOutcome
> = true;

const _authorControllableAreOutcomes: AssertEqual<
  Exclude<AuthorControllableOutcome, InboundMailOutcome>,
  never
> = true;
