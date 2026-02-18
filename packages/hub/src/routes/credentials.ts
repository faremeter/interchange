import { Hono } from "hono";
import { describeRoute, resolver, validator } from "hono-openapi";
import {
  CreateCredential,
  UpdateCredential,
  CredentialResponse,
  ErrorResponse,
} from "@interchange/types";

import type { AppEnv } from "../context";

const app = new Hono<AppEnv>();

app.get(
  "/",
  describeRoute({
    tags: ["Credentials"],
    summary: "List credentials",
    description:
      "Lists credential metadata. Secrets are never returned. Access for agents is managed through capability grants.",
    responses: {
      200: {
        description: "List of credentials",
        content: {
          "application/json": {
            schema: resolver(CredentialResponse.array()),
          },
        },
      },
    },
  }),
  (c) =>
    c.json(
      { error: { code: "not_implemented", message: "Not implemented" } },
      501,
    ),
);

app.post(
  "/",
  describeRoute({
    tags: ["Credentials"],
    summary: "Store a credential",
    description:
      "Stores a credential (API key, OAuth token, etc.). The secret is stored securely and never returned in subsequent reads.",
    responses: {
      201: {
        description: "Credential stored",
        content: {
          "application/json": { schema: resolver(CredentialResponse) },
        },
      },
      400: {
        description: "Validation error",
        content: {
          "application/json": { schema: resolver(ErrorResponse) },
        },
      },
    },
  }),
  validator("json", CreateCredential),
  (c) =>
    c.json(
      { error: { code: "not_implemented", message: "Not implemented" } },
      501,
    ),
);

app.get(
  "/:credentialId",
  describeRoute({
    tags: ["Credentials"],
    summary: "Get credential metadata",
    description: "Returns credential metadata. The secret is never included.",
    responses: {
      200: {
        description: "Credential metadata",
        content: {
          "application/json": { schema: resolver(CredentialResponse) },
        },
      },
      404: {
        description: "Credential not found",
        content: {
          "application/json": { schema: resolver(ErrorResponse) },
        },
      },
    },
  }),
  (c) =>
    c.json(
      { error: { code: "not_implemented", message: "Not implemented" } },
      501,
    ),
);

app.patch(
  "/:credentialId",
  describeRoute({
    tags: ["Credentials"],
    summary: "Rotate or update a credential",
    responses: {
      200: {
        description: "Credential updated",
        content: {
          "application/json": { schema: resolver(CredentialResponse) },
        },
      },
      404: {
        description: "Credential not found",
        content: {
          "application/json": { schema: resolver(ErrorResponse) },
        },
      },
    },
  }),
  validator("json", UpdateCredential),
  (c) =>
    c.json(
      { error: { code: "not_implemented", message: "Not implemented" } },
      501,
    ),
);

app.delete(
  "/:credentialId",
  describeRoute({
    tags: ["Credentials"],
    summary: "Revoke a credential",
    responses: {
      204: {
        description: "Credential revoked",
      },
      404: {
        description: "Credential not found",
        content: {
          "application/json": { schema: resolver(ErrorResponse) },
        },
      },
    },
  }),
  (c) =>
    c.json(
      { error: { code: "not_implemented", message: "Not implemented" } },
      501,
    ),
);

export { app as credentialRoutes };
