import { type } from "arktype";

export const sidecarStatuses = ["online", "offline", "error"] as const;
export type SidecarStatus = (typeof sidecarStatuses)[number];

const Status = type.enumerated(...sidecarStatuses);

export const CreateSidecar = type({
  "id?": "string > 0",
  url: "string",
  "status?": Status,
}).describe(
  "Register or update a sidecar. The id field supports idempotent registration by a known stable identifier; if omitted, the server generates one.",
);

export const SidecarResponse = type({
  id: "string",
  url: "string",
  status: Status,
  lastHeartbeat: "string | null",
  createdAt: "string",
  updatedAt: "string",
});
