import { customType } from "drizzle-orm/pg-core";

// Postgres `bytea` mapped to `Uint8Array` on both the app and driver
// sides, so binary columns (hash digests, raw message frames) round-trip
// as raw bytes with no encoding layer to keep in sync.
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
