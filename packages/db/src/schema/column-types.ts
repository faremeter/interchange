import { customType } from "drizzle-orm/pg-core";

// Postgres `bytea` mapped to `Uint8Array` on both sides, so binary columns
// (hash digests, raw message frames) round-trip as raw bytes.
export const bytea = customType<{ data: Uint8Array; driverData: Uint8Array }>({
  dataType() {
    return "bytea";
  },
  toDriver(value) {
    return value;
  },
  fromDriver(value) {
    return new Uint8Array(value);
  },
});
