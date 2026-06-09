// Minimal hand-rolled declarations for npm-team packages that ship no
// @types and no first-party TypeScript definitions. We declare only the
// surface this package actually consumes; broader coverage belongs
// upstream.

declare module "npm-pick-manifest" {
  interface PackumentVersion {
    name: string;
    version: string;
    dist: {
      tarball: string;
      integrity?: string;
      shasum?: string;
    };
    dependencies?: Record<string, string>;
    optionalDependencies?: Record<string, string>;
    peerDependencies?: Record<string, string>;
    os?: string[];
    cpu?: string[];
  }

  interface Packument {
    name: string;
    "dist-tags"?: Record<string, string>;
    versions: Record<string, PackumentVersion>;
  }

  interface PickManifestOptions {
    defaultTag?: string;
    before?: string | Date;
    nodeVersion?: string;
    includeStaged?: boolean;
  }

  function pickManifest(
    packument: Packument,
    wanted: string,
    opts?: PickManifestOptions,
  ): PackumentVersion;

  export = pickManifest;
}

declare module "npm-registry-fetch" {
  interface FetchOptions {
    registry?: string;
    headers?: Record<string, string>;
    forceAuth?: Record<string, string>;
    token?: string;
    [key: string]: unknown;
  }

  function fetch(url: string, opts?: FetchOptions): Promise<Response>;
  namespace fetch {
    function json<T = unknown>(url: string, opts?: FetchOptions): Promise<T>;
  }

  export = fetch;
}
