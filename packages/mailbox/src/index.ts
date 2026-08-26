export {
  DEFAULT_MAILBOXES,
  createInMemoryMailboxStore,
  requireMessage,
} from "./mailbox";
export type { MailboxStore, StoredMessage, StoredEnvelope } from "./mailbox";

export { executeSearch } from "./search";
export { executeThread } from "./thread";
export { fetchHeaders, fetchStructure, fetchPart, fetchFull } from "./fetch";
export { buildMessageHeaders } from "./headers";
