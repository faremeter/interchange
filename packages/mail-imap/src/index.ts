export { createImapTransport } from "./transport";
export type {
  ImapEndpoint,
  ImapTransport,
  ImapTransportConfig,
  SmtpEndpoint,
} from "./transport";

export { createImapHubTransport } from "./hub-transport";
export type {
  ImapHubTransport,
  ImapHubTransportConfig,
  IngestOutcome,
} from "./hub-transport";

export {
  createCommandMailboxProvisioner,
  deriveMailboxPassword,
} from "./provisioner";
export type {
  CommandMailboxProvisionerConfig,
  MailAccountCredentials,
  MailboxProvisioner,
} from "./provisioner";

export {
  createSmtpRelay,
  describeSubmitFailure,
  isTransientSubmitFailure,
  submitWithRetry,
  DEFAULT_SUBMIT_ATTEMPTS,
} from "./relay";
export type { SmtpRelay, SmtpRelayConfig } from "./relay";

export { retrying, realSleep, DEFAULT_RETRY_BASE_MS } from "./retry";
export type { RetryOptions, RetryPolicy } from "./retry";

export { createFetchedStore } from "./fetched-store";
export type { FetchedMessage, MailboxCounters } from "./fetched-store";

export { translate as translateSearchQuery } from "./search-criteria";
export type { TranslatedSearch } from "./search-criteria";
