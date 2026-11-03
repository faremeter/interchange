/**
 * Hub-owned refs that keep commits reachable without showing up as branches, so
 * a prune or a manual `git gc` cannot lose them.
 */
export const HUB_REF_NAMESPACE = "refs/interchange";
