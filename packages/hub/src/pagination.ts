import {
  type SQL,
  type SQLWrapper,
  and,
  desc,
  lt,
  eq,
  or,
  sql,
} from "drizzle-orm";
import { type } from "arktype";

const DEFAULT_LIMIT = 50;
const MAX_LIMIT = 100;

const CursorData = type({
  t: /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/,
  id: "string",
});
type CursorData = typeof CursorData.infer;

function encodeCursor(createdAt: Date, id: string): string {
  const data: CursorData = { t: createdAt.toISOString(), id };
  return Buffer.from(JSON.stringify(data)).toString("base64url");
}

function decodeCursor(cursor: string): CursorData | null {
  try {
    const json = Buffer.from(cursor, "base64url").toString();
    const parsed: unknown = JSON.parse(json);
    const data = CursorData(parsed);
    if (data instanceof type.errors) return null;
    return data;
  } catch {
    return null;
  }
}

export function parsePageParams(query: {
  cursor: string | undefined;
  limit: string | undefined;
}): {
  limit: number;
  cursor: CursorData | null;
} {
  const raw = query.limit ? Number(query.limit) : DEFAULT_LIMIT;
  const limit = Number.isNaN(raw)
    ? DEFAULT_LIMIT
    : Math.min(Math.max(1, raw), MAX_LIMIT);

  const cursor = query.cursor ? decodeCursor(query.cursor) : null;
  return { limit, cursor };
}

/**
 * Builds the WHERE clause fragment for cursor-based keyset pagination.
 * Ordering is createdAt DESC, id DESC (newest first).
 *
 * The cursor condition is: (createdAt < cursor.t) OR (createdAt = cursor.t AND id < cursor.id)
 */
export function cursorCondition(
  createdAtCol: SQLWrapper,
  idCol: SQLWrapper,
  cursor: CursorData,
): SQL {
  // Both branches always produce valid SQL, so or() never returns undefined
  return (
    or(
      lt(createdAtCol, new Date(cursor.t)),
      and(eq(createdAtCol, new Date(cursor.t)), lt(idCol, cursor.id)),
    ) ?? sql`false`
  );
}

/**
 * Returns the ORDER BY clause for pagination: createdAt DESC, id DESC.
 */
export function pageOrder(createdAtCol: SQLWrapper, idCol: SQLWrapper): SQL[] {
  return [desc(createdAtCol), desc(idCol)];
}

/**
 * Wraps a list of formatted items into a paginated response envelope.
 * Accepts the raw DB rows (with createdAt and id) alongside the
 * formatted items so it can derive the next cursor.
 */
export function paginatedResponse<T>(
  items: T[],
  rows: { createdAt: Date; id: string }[],
  limit: number,
): { data: T[]; nextCursor: string | null } {
  const hasMore = items.length === limit;
  const lastRow = hasMore ? rows[rows.length - 1] : undefined;
  return {
    data: items,
    nextCursor: lastRow ? encodeCursor(lastRow.createdAt, lastRow.id) : null,
  };
}

/**
 * OpenAPI parameter definitions for cursor-based pagination.
 * Spread these into the `parameters` array of `describeRoute`.
 */
export const pageParameters = [
  {
    name: "cursor",
    in: "query" as const,
    description: "Opaque pagination cursor from a previous response",
    schema: { type: "string" as const },
  },
  {
    name: "limit",
    in: "query" as const,
    description: "Maximum number of results (1-100, default 50)",
    schema: { type: "integer" as const, minimum: 1, maximum: 100 },
  },
];
