import { type } from "arktype";

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
  items: "unknown[]",
  "nextCursor?": "string | null",
});

export const Timestamps = type({
  createdAt: "string",
  updatedAt: "string",
});
