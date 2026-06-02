import { type, type Type } from "arktype";

export const ErrorResponse = type({
  error: {
    code: "string",
    message: "string",
  },
});

export const PaginationParams = type({
  "cursor?": "string",
  "limit?": "string",
});

export const PaginatedList = type({
  data: "unknown[]",
  nextCursor: "string | null",
});

/**
 * Creates a typed paginated response schema for use with OpenAPI.
 * Wraps an item array schema in `{ data: T[], nextCursor: string | null }`.
 */
export function paginatedSchema<T>(itemSchema: Type<T>) {
  return type({
    data: itemSchema.array(),
    nextCursor: "string | null",
  });
}

export const Timestamps = type({
  createdAt: "string",
  updatedAt: "string",
});
