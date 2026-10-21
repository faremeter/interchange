/**
 * Validate the hub API origin a sidecar reaches the REST API at, and reduce it
 * to its origin. Refuses anything but an http or https URL, so the hub does
 * not start with run tokens pinned to an unusable origin. A consumer fetches
 * absolute paths against the origin, so a path, credentials, a query or a
 * fragment would be dropped. `source` names the setting the value came from,
 * for the refusal. With `onMoreThanOrigin`, a value that carries more than an
 * origin calls it instead of being refused: the auth base URL may carry a
 * path for its own use.
 */
export function resolveHubApiOrigin(
  source: string,
  value: string,
  onMoreThanOrigin?: () => void,
): string {
  let url: URL;
  try {
    url = new URL(value);
  } catch {
    throw new Error(`${source} ${JSON.stringify(value)} is not a URL`);
  }
  if (url.protocol !== "http:" && url.protocol !== "https:") {
    throw new Error(
      `${source} ${JSON.stringify(value)} must be an http or https URL, as the hub API origin`,
    );
  }
  const moreThanOrigin =
    url.username !== "" ||
    url.password !== "" ||
    url.pathname !== "/" ||
    url.search !== "" ||
    url.hash !== "";
  if (moreThanOrigin) {
    if (onMoreThanOrigin === undefined) {
      throw new Error(
        `${source} ${JSON.stringify(value)} must be an origin, with no path, credentials, query or fragment`,
      );
    }
    onMoreThanOrigin();
  }
  return url.origin;
}
