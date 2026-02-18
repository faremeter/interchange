import { Hono } from "hono";
import { describeRoute, resolver, validator } from "hono-openapi";
import {
  CreateTenant,
  UpdateTenant,
  TenantResponse,
  FederationTrust,
  CreateFederationTrust,
  ErrorResponse,
} from "@interchange/types";

import type { AppEnv } from "../context";

const app = new Hono<AppEnv>();

app.post(
  "/",
  describeRoute({
    tags: ["Tenants"],
    summary: "Create a tenant",
    description:
      "Creates a new tenant. The authenticated user becomes the owner with a principal and default owner role.",
    responses: {
      201: {
        description: "Tenant created",
        content: {
          "application/json": { schema: resolver(TenantResponse) },
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
  validator("json", CreateTenant),
  (c) =>
    c.json(
      { error: { code: "not_implemented", message: "Not implemented" } },
      501,
    ),
);

app.get(
  "/:tenantId",
  describeRoute({
    tags: ["Tenants"],
    summary: "Get tenant details",
    responses: {
      200: {
        description: "Tenant details",
        content: {
          "application/json": { schema: resolver(TenantResponse) },
        },
      },
      403: {
        description: "Not a member of this tenant",
        content: {
          "application/json": { schema: resolver(ErrorResponse) },
        },
      },
      404: {
        description: "Tenant not found",
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
  "/:tenantId",
  describeRoute({
    tags: ["Tenants"],
    summary: "Update tenant config",
    description: "Requires admin or higher grant within the tenant.",
    responses: {
      200: {
        description: "Tenant updated",
        content: {
          "application/json": { schema: resolver(TenantResponse) },
        },
      },
      403: {
        description: "Insufficient grants",
        content: {
          "application/json": { schema: resolver(ErrorResponse) },
        },
      },
    },
  }),
  validator("json", UpdateTenant),
  (c) =>
    c.json(
      { error: { code: "not_implemented", message: "Not implemented" } },
      501,
    ),
);

app.get(
  "/:tenantId/federation",
  describeRoute({
    tags: ["Tenants"],
    summary: "List federation trust relationships",
    responses: {
      200: {
        description: "Federation trusts",
        content: {
          "application/json": {
            schema: resolver(FederationTrust.array()),
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
  "/:tenantId/federation",
  describeRoute({
    tags: ["Tenants"],
    summary: "Establish federation trust",
    description:
      "Creates a trust relationship with another tenant for cross-tenant agent discovery and interaction.",
    responses: {
      201: {
        description: "Trust established",
        content: {
          "application/json": { schema: resolver(FederationTrust) },
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
  validator("json", CreateFederationTrust),
  (c) =>
    c.json(
      { error: { code: "not_implemented", message: "Not implemented" } },
      501,
    ),
);

app.delete(
  "/:tenantId/federation/:targetTenantId",
  describeRoute({
    tags: ["Tenants"],
    summary: "Revoke federation trust",
    responses: {
      204: {
        description: "Trust revoked",
      },
      404: {
        description: "Trust not found",
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

export { app as tenantRoutes };
