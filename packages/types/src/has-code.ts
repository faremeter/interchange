// Type guard for errors with a Node-style `{ code: string }` shape,
// as thrown by Node.js (POSIX errno), isomorphic-git, and similar.

export function hasCode(err: unknown): err is { code: string } {
  return (
    typeof err === "object" &&
    err !== null &&
    "code" in err &&
    typeof (err as { code: unknown }).code === "string"
  );
}
