import { isNamespaceResource } from "./authz";
import { MAIL_ACCEPT_NAMESPACE } from "./mail-accept";
import { MAIL_SEND_NAMESPACE } from "./mail-send";

/**
 * A grant namespace whose rows come only from fixed sources, never from a
 * workflow's grant requirements. `source` names those sources, for a refusal
 * to tell the author where such rows come from.
 */
export type ReservedGrantNamespace = {
  readonly namespace: string;
  readonly source: string;
};

/**
 * Grant namespaces a workflow may not declare a grant requirement in. A
 * requirement materializes a delegated grant, and a delegator that holds `*`
 * satisfies any requirement, so a requirement inside one of these namespaces
 * would create rows outside the sources that the namespace allows.
 */
export const RESERVED_GRANT_NAMESPACES: readonly ReservedGrantNamespace[] = [
  {
    namespace: MAIL_ACCEPT_NAMESPACE,
    source: "accept-rules are declared only through mailAccept",
  },
  {
    namespace: MAIL_SEND_NAMESPACE,
    source:
      "send grants come only from the hub at a run's first trigger and " +
      "from operator grants",
  },
];

/**
 * The reserved namespace that `resource` lies in, or `null` when it lies in
 * none of them.
 */
export function reservedGrantNamespace(
  resource: string,
): ReservedGrantNamespace | null {
  return (
    RESERVED_GRANT_NAMESPACES.find((reserved) =>
      isNamespaceResource(reserved.namespace, resource),
    ) ?? null
  );
}
