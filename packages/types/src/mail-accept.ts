import { type } from "arktype";

export const MAIL_ACCEPT_NAMESPACE = "mail.accept";

/**
 * Whether a grant resource lies inside the `mail.accept` namespace: the bare
 * namespace or any `mail.accept:<...>` resource. This is exactly the set of
 * grants the inbound admission gate evaluates; a broader grant such as `*` or
 * `mail.*` matches `mail.accept:...` under pattern matching but falls outside
 * this set, so the gate never lets it admit mail.
 */
export function isMailAcceptNamespaceResource(resource: string): boolean {
  return (
    resource === MAIL_ACCEPT_NAMESPACE ||
    resource.startsWith(`${MAIL_ACCEPT_NAMESPACE}:`)
  );
}

/**
 * A `mail.accept` coordinate id is valid when it is a non-empty string with no
 * `:` and no `*`. The colon is the segment separator of the
 * `mail.accept:<type>:<id>` resource, so an id that carries one would split
 * into extra segments and make the shape ambiguous. The `*` is the wildcard of
 * grant matching, so an id that carries one would be a pattern that admits
 * many senders, not the one sender it names. This owns that rule: the resource
 * builder in `@intx/authz` imports it, and the `MailAccept` schema narrows on
 * it, so the authoring boundary and the resource builder share one definition.
 */
export function isValidCoordId(id: string): boolean {
  return id.length > 0 && !id.includes(":") && !id.includes("*");
}

/**
 * A per-workflow relational accept-rule set: the author's declaration of which
 * senders a deployment accepts inbound mail from, keyed by the sender's relation
 * to the deployment. The relational toggles admit a sender the deployment stands
 * in that relation to:
 *
 * - `invoker` -- the principal that started the run: the HTTP caller, or for
 *   a mail trigger the sender whose mail fired it. A deployment has one run,
 *   so mail from any other sender needs another rule.
 * - `self` -- a deployment of any definition backed by the same workflow
 *   asset. One asset backs many definitions (one per wire hash), so this
 *   trusts every definition the asset backs, not only other versions of this
 *   one. The rule stays valid when the definition's content changes.
 * - `tenant` -- any principal in the same tenant: its users and its
 *   deployments.
 * - `correspondent` -- the reply from a party the deployment mailed.
 *
 * `principals` and `assets` name explicit senders by id: a principal id the
 * deployment accepts regardless of relation, and a workflow asset id whose
 * deployments the author accepts. An asset id admits a deployment of every
 * definition that the asset backs. Both id kinds are global, so an explicit id
 * can name a sender in another tenant.
 *
 * Enforcement: `invoker`, `self`, `tenant`, and the explicit
 * `principals`/`assets` are resolved at launch into `mail.accept` grant
 * rows; `correspondent` is minted at the send instant when the deployment mails
 * a party, so that party's reply is admitted. Every relation the schema accepts
 * is enforced by the inbound admission gate -- there is no declarable-but-inert
 * relation.
 *
 * The object is SPARSE: every key is optional, and an omitted key is NOT a
 * default of any kind here. Each relation is opt-in: an absent toggle resolves
 * OFF in the shared accept-rule resolver, so a definition accepts nothing on the
 * relational axis unless it declares a relation. Keeping the field sparse means
 * the content hash covers only what the author actually declared, so a
 * definition that omits the field hashes identically to one authored before the
 * field existed. Undeclared keys are rejected so a typo such as `invokr` or
 * `principal` fails at the wire boundary rather than riding through as an inert
 * unknown key.
 */
export const MailAccept = type({
  "invoker?": "boolean",
  "self?": "boolean",
  "tenant?": "boolean",
  "correspondent?": "boolean",
  "principals?": "string[]",
  "assets?": "string[]",
})
  .onUndeclaredKey("reject")
  // A raw wire definition reaches this schema without passing through
  // `defineWorkflow`, so an authored id that violates the coordinate grammar is
  // untrusted input that must be rejected here rather than throwing later in the
  // capability walk. Reject an explicit `principals`/`assets` id that could
  // not compose into a `mail.accept:<type>:<id>` resource.
  .narrow((accept, ctx) => {
    for (const key of ["principals", "assets"] as const) {
      const ids = accept[key];
      if (ids === undefined) continue;
      for (const id of ids) {
        if (!isValidCoordId(id)) {
          return ctx.mustBe(
            `a non-empty ${key} id without ":" or "*" (received ${JSON.stringify(id)})`,
          );
        }
      }
    }
    return true;
  });
export type MailAccept = typeof MailAccept.infer;

/**
 * The relational keys of `MailAccept`: every boolean toggle the schema
 * declares. Derived from the schema, so a relation added to it is a relation
 * here, and the shared resolver fails the type check until it handles it.
 */
export type MailAcceptRelation = {
  [K in keyof MailAccept]-?: NonNullable<MailAccept[K]> extends boolean
    ? K
    : never;
}[keyof MailAccept];
