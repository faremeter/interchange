// Seeds tenant memberships beyond the tenant's creator into a spawned hub's
// schema. The hub has no route that adds an active member directly, so the
// principal row and its role assignment are written with raw SQL.

import postgres from "postgres";

import { generateId } from "@intx/hub-common";
import { loadHarnessDbConfig } from "@intx/test-harness/db-harness";

import type { SignedUpUser } from "./git-asset-fixtures";

export type MembershipStatus =
  | "active"
  | "suspended"
  | "invited"
  | "deactivated";

export async function addMembership(
  schema: string,
  tenantId: string,
  user: SignedUpUser,
  roleName: "admin" | "member",
  status: MembershipStatus,
): Promise<void> {
  const dbConfig = loadHarnessDbConfig();
  const sql = postgres({
    host: dbConfig.host,
    port: dbConfig.port,
    user: dbConfig.user,
    password: dbConfig.password,
    database: dbConfig.database,
    max: 1,
    connection: { search_path: `"${schema.replace(/"/g, '""')}"` },
  });
  try {
    const principalId = generateId("principal");
    await sql`insert into principal (id, tenant_id, kind, ref_id, status)
              values (${principalId}, ${tenantId}, 'user', ${user.userId}, ${status})`;
    const assigned =
      await sql`insert into principal_role (principal_id, role_id)
              select ${principalId}, id from role
              where tenant_id = ${tenantId} and name = ${roleName}
              returning role_id`;
    if (assigned.length !== 1) {
      throw new Error(
        `addMembership: tenant ${tenantId} has no ${roleName} role to assign`,
      );
    }
  } finally {
    await sql.end();
  }
}
