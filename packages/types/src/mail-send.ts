import { isNamespaceResource } from "./authz";

export const MAIL_SEND_NAMESPACE = "mail.send";

/**
 * Whether a grant resource lies inside the `mail.send` namespace. This is
 * exactly the set of grants the outbound send gate evaluates.
 */
export function isMailSendNamespaceResource(resource: string): boolean {
  return isNamespaceResource(MAIL_SEND_NAMESPACE, resource);
}
