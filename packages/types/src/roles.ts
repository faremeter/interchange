import { type } from "arktype";

export const CreateRole = type({
  name: "string",
  "description?": "string",
});

export const UpdateRole = type({
  "name?": "string",
  "description?": "string",
});

export const RoleResponse = type({
  id: "string",
  tenantId: "string",
  name: "string",
  "description?": "string | null",
  isSystem: "boolean",
  createdAt: "string",
  updatedAt: "string",
});
