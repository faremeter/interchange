import { sql } from "drizzle-orm";
import {
  boolean,
  check,
  integer,
  jsonb,
  pgTable,
  text,
  timestamp,
  unique,
} from "drizzle-orm/pg-core";

import { credential } from "./credentials";
import { tenant } from "./tenants";
import { wallet } from "./wallets";

export const model = pgTable(
  "model",
  {
    id: text("id").primaryKey(),
    tenantId: text("tenant_id")
      .notNull()
      .references(() => tenant.id, { onDelete: "cascade" }),
    canonicalName: text("canonical_name").notNull(),
    displayName: text("display_name"),
    description: text("description"),
    // Own-row disable. Suppressing an inherited row (a child tenant hiding a
    // parent's model) is a separate resolution-time mechanism, not this flag.
    disabled: boolean("disabled").notNull().default(false),
    createdAt: timestamp("created_at").notNull().defaultNow(),
    updatedAt: timestamp("updated_at").notNull().defaultNow(),
  },
  (t) => [
    unique("model_tenant_canonical_name").on(t.tenantId, t.canonicalName),
  ],
);

export const modelProvider = pgTable(
  "model_provider",
  {
    id: text("id").primaryKey(),
    tenantId: text("tenant_id")
      .notNull()
      .references(() => tenant.id, { onDelete: "cascade" }),
    name: text("name").notNull(),
    plugin: text("plugin", {
      enum: ["anthropic", "openai", "openai-compatible", "google-genai"],
    }).notNull(),
    baseURL: text("base_url").notNull(),
    // A provider authenticates via exactly one of credential or wallet (the
    // check constraint below enforces the XOR). onDelete is "restrict" so a
    // credential or wallet cannot be deleted out from under a provider that
    // depends on it — the provider must be repointed or removed first.
    credentialId: text("credential_id").references(() => credential.id, {
      onDelete: "restrict",
    }),
    walletId: text("wallet_id").references(() => wallet.id, {
      onDelete: "restrict",
    }),
    // Own-row disable. Inherited-row suppression is resolved separately.
    disabled: boolean("disabled").notNull().default(false),
    createdAt: timestamp("created_at").notNull().defaultNow(),
    updatedAt: timestamp("updated_at").notNull().defaultNow(),
  },
  (t) => [
    unique("model_provider_tenant_name").on(t.tenantId, t.name),
    check(
      "model_provider_auth_xor",
      sql`(${t.credentialId} is not null) <> (${t.walletId} is not null)`,
    ),
  ],
);

export const modelOffering = pgTable(
  "model_offering",
  {
    id: text("id").primaryKey(),
    tenantId: text("tenant_id")
      .notNull()
      .references(() => tenant.id, { onDelete: "cascade" }),
    modelId: text("model_id")
      .notNull()
      .references(() => model.id, { onDelete: "cascade" }),
    providerId: text("provider_id")
      .notNull()
      .references(() => modelProvider.id, { onDelete: "cascade" }),
    priority: integer("priority").notNull(),
    deploymentTags: text("deployment_tags").array().notNull().default([]),
    // Curated-capability tags. This issue creates the column; the values are
    // populated by the discovery-support-matrix seeding work.
    capabilities: text("capabilities").array().notNull().default([]),
    // Opaque per-deployment bag of provider-specific adapter accommodations
    // (e.g. OpenAI's forceAssistantReasoningContent). Interpreted and
    // validated only at the adapter factory, which alone knows the provider
    // shape. NULL means the factory receives no bag and applies its default
    // behavior.
    quirks: jsonb("quirks"),
    // Own-row disable. Inherited-row suppression is resolved separately.
    disabled: boolean("disabled").notNull().default(false),
    createdAt: timestamp("created_at").notNull().defaultNow(),
    updatedAt: timestamp("updated_at").notNull().defaultNow(),
  },
  (t) => [
    unique("model_offering_tenant_model_provider").on(
      t.tenantId,
      t.modelId,
      t.providerId,
    ),
  ],
);

export const modelPricing = pgTable(
  "model_pricing",
  {
    id: text("id").primaryKey(),
    tenantId: text("tenant_id")
      .notNull()
      .references(() => tenant.id, { onDelete: "cascade" }),
    offeringId: text("offering_id")
      .notNull()
      .references(() => modelOffering.id, { onDelete: "cascade" }),
    currency: text("currency").notNull(),
    inputTokenPrice: text("input_token_price"),
    outputTokenPrice: text("output_token_price"),
    cacheReadTokenPrice: text("cache_read_token_price"),
    cacheWriteTokenPrice: text("cache_write_token_price"),
    thinkingTokenPrice: text("thinking_token_price"),
    perRequestFee: text("per_request_fee"),
    perImageFee: text("per_image_fee"),
    perAudioFee: text("per_audio_fee"),
    // Append-only. A price change inserts a new row with a later
    // effective_from; rows are never edited in place, so cost attribution at
    // a past timestamp stays accurate. withTimezone anchors the as-of
    // comparison across writers.
    effectiveFrom: timestamp("effective_from", {
      withTimezone: true,
    }).notNull(),
    createdAt: timestamp("created_at", { withTimezone: true })
      .notNull()
      .defaultNow(),
  },
  (t) => [
    unique("model_pricing_offering_currency_effective_from").on(
      t.offeringId,
      t.currency,
      t.effectiveFrom,
    ),
  ],
);
