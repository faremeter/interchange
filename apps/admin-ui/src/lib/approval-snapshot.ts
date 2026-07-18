export type ApprovalSnapshotView =
  | {
      ok: true;
      name: string;
      description: string;
      inputSchema: unknown;
      arguments: Record<string, unknown>;
    }
  | { ok: false };

/**
 * Narrow an approval's opaque wire snapshot into the fields the panel renders.
 * `toolDefinition` and `toolArguments` cross `fetch` as `Record<string,
 * unknown>` -- the API validates them as opaque records, not a structured
 * shape -- so the tool name and description are untrusted here. Validate them
 * as strings before rendering and report a malformed snapshot rather than
 * papering a missing field into a blank. `inputSchema` and `arguments` are
 * rendered as JSON, so their internal shape does not need narrowing.
 */
export function parseApprovalSnapshot(
  toolDefinition: Record<string, unknown>,
  toolArguments: Record<string, unknown>,
): ApprovalSnapshotView {
  const { name, description, inputSchema } = toolDefinition;
  if (typeof name !== "string" || typeof description !== "string") {
    return { ok: false };
  }
  return { ok: true, name, description, inputSchema, arguments: toolArguments };
}
