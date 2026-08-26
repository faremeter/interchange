# @intx/mailbox

Storage-agnostic IMAP mailbox model. A `MailboxStore` backing owns how a
mailbox's message list and its uid/modseq/uidValidity counters are stored; the
pure query and projection functions read the message snapshot the backing
exposes.

The package ships one reference backing, `createInMemoryMailboxStore`, which
keeps messages and counters in process memory. Other backings (for example a
persistent substrate) implement the same `MailboxStore` interface, so the
search, threading, and fetch logic is written once and reused across every
backing.

```ts
import {
  createInMemoryMailboxStore,
  executeSearch,
  fetchFull,
} from "@intx/mailbox";

const store = createInMemoryMailboxStore();
const uid = store.append(rawMessageBytes, envelope, []);

const hits = executeSearch("INBOX", store, { from: "alpha@local.interchange" });
const message = await fetchFull({ uid, mailbox: "INBOX" }, store, getCrypto);
```

The projections parse from the stored RFC 2822 bytes, so
`@intx/mailbox` produces the same envelope, structure, and signature results
regardless of which backing holds the message.
