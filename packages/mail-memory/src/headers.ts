// `buildMessageHeaders` now lives in `@intx/mime` alongside the rest of the
// MIME/header parsing (it is also what the `decodeMail` decoder builds its
// typed header subset with). Re-exported here so mail-memory's callers keep
// their existing import path.
export { buildMessageHeaders } from "@intx/mime";
