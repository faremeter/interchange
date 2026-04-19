# Faremeter Interchange

_Message Transport_

The Interchange message transport is built on SMTP and IMAP as wire protocols. Agents send messages via SMTP and manage their inbox via IMAP. The transport layer is abstracted behind an interface that captures the full semantics of both protocols, allowing implementations to range from real SMTP/IMAP servers to in-process stubs that route messages through memory.

This document specifies the message format, the IMAP inbox model, and the transport interface. It is the contract between the harness (which uses the interface) and the transport implementations (which provide it).

## Agent Addressing

Every agent has an SMTP address as its network identity:

```
agent-name@tenant.interchange.network
```

The local part identifies the agent within a tenant. The domain identifies the tenant. Tenant boundaries map directly to SMTP domains, providing natural isolation and federation semantics. DNS MX records route messages to the correct infrastructure.

Address format follows RFC 5322 addr-spec: `local-part "@" domain`. The local part is a dot-atom (letters, digits, hyphens, dots). No quoted strings, no special characters. Agent names are assigned at creation time and are unique within a tenant.

## Message Format

Every Interchange message is a PGP/MIME signed (RFC 3156) multipart message. The signature provides content provenance — recipients verify that the message was produced by the claimed sender's Ed25519 key.

### MIME Structure

The outer layer is always `multipart/signed` per RFC 3156. The signed content is the first part; the detached PGP signature is the second part, computed after canonicalization (trailing whitespace removed, line endings normalized to CRLF, content constrained to 7-bit).

The signed content varies by message type. Conversation messages use `text/plain` directly — they are just signed emails, readable by any mail client. Structured messages (offerings, payments, approvals, system) use `multipart/mixed` with an `application/vnd.interchange+json` part carrying machine-readable data.

**Conversation messages:**

```
multipart/signed; protocol="application/pgp-signature"; micalg=pgp-sha512
├── text/plain                              [the message]
└── application/pgp-signature               [Ed25519 detached signature]
```

**Structured messages:**

```
multipart/signed; protocol="application/pgp-signature"; micalg=pgp-sha512
├── multipart/mixed
│   ├── application/vnd.interchange+json    [structured payload]
│   ├── text/plain (optional)               [human-readable summary]
│   └── [additional parts] (optional)       [attachments, images, artifacts]
└── application/pgp-signature               [Ed25519 detached signature]
```

The `Interchange-Type` header determines which shape to expect. Conversation types (`conversation.message`, `conversation.join`, `conversation.leave`) use the plain text form. All other types use the structured form.

### Part Addressing

IMAP FETCH addresses MIME parts by position using dot-separated numeric paths (RFC 9051). Part paths differ between the two MIME shapes.

**Conversation messages:**

| IMAP Part | Content                         |
| --------- | ------------------------------- |
| `1`       | The `text/plain` message body   |
| `2`       | The `application/pgp-signature` |

**Structured messages:**

| IMAP Part | Content                                       |
| --------- | --------------------------------------------- |
| `1`       | The `multipart/mixed` payload (all sub-parts) |
| `1.1`     | The `application/vnd.interchange+json` part   |
| `1.2`     | The `text/plain` summary (if present)         |
| `1.3+`    | Attachments (if present)                      |
| `2`       | The `application/pgp-signature`               |

For conversation messages, `BODY[1]` fetches the text. For structured messages, `BODY[1.1]` fetches just the JSON payload without downloading attachments. In both cases, `BODY[2]` fetches the signature for verification.

### Headers

Every message carries standard RFC 5322 headers plus Interchange-specific headers.

**Standard headers (RFC 5322):**

| Header         | Usage                                                                                         |
| -------------- | --------------------------------------------------------------------------------------------- |
| `From`         | Sender's agent address                                                                        |
| `To`           | Recipient address(es). Multiple recipients for 1:N broadcast.                                 |
| `Cc`           | Additional recipients (visible to all)                                                        |
| `Date`         | Origination timestamp (RFC 5322 date-time format)                                             |
| `Message-ID`   | Unique identifier: `<uuid@tenant.interchange.network>`                                        |
| `In-Reply-To`  | Message-ID of the parent message (for threading)                                              |
| `References`   | Full ancestry chain: parent's References + parent's Message-ID                                |
| `Subject`      | Conversation topic or offering name                                                           |
| `MIME-Version` | Always `1.0`                                                                                  |
| `Content-Type` | Always `multipart/signed; ...` at the top level                                               |
| `List-ID`      | Distribution list identifier for M:N conversations (RFC 2919). Present only on list messages. |

**Trace context headers (W3C Trace Context):**

| Header        | Usage                                                                     |
| ------------- | ------------------------------------------------------------------------- |
| `traceparent` | W3C trace context propagation (version, trace-id, parent-id, trace-flags) |
| `tracestate`  | Vendor-specific trace data for observability tool interop                 |

Distributed tracing across agent boundaries uses the W3C Trace Context standard (registered headers, supported by OpenTelemetry and most observability tools). The harness sets `traceparent` on every outbound message. The receiving harness extracts it and continues the trace span.

**Interchange headers:**

| Header                       | Usage                                                                                                                                                                                                                                                                                                |
| ---------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `Interchange-Type`           | Message type for routing without body parsing (see Payload Types). Mirrors the `type` field in the JSON payload. The payload body is authoritative on conflict.                                                                                                                                      |
| `Interchange-Correlation-ID` | Links a response to the request that triggered it                                                                                                                                                                                                                                                    |
| `Interchange-Tenant-ID`      | Sender's tenant identifier                                                                                                                                                                                                                                                                           |
| `Interchange-Agent-ID`       | Sender's agent identifier (distinct from SMTP address)                                                                                                                                                                                                                                               |
| `Interchange-Session-ID`     | Reactor session that produced this message. Enables the receiving harness to correlate inbound messages with an active session channel for event routing and observability. Present when the sending agent has an active reactor; absent for system-level messages that originate outside a session. |
| `Interchange-Offering-ID`    | Offering being invoked or responded to. Mirrors `body.offeringId` in `offering.*` payloads. The payload body is authoritative on conflict.                                                                                                                                                           |
| `Interchange-Schema-Version` | Payload schema version for forward compatibility                                                                                                                                                                                                                                                     |

Interchange headers use the `Interchange-` prefix. RFC 6648 deprecated the `X-` convention but did not prescribe a replacement for application-specific headers. These headers are scoped to the Interchange ecosystem and are not currently registered with IANA. Registration should happen when the protocol stabilizes for federation with external systems.

### Payload Types

The `Interchange-Type` header identifies every message's type for routing without body parsing. Conversation types use `text/plain` bodies. All other types use `application/vnd.interchange+json` with a JSON object whose `type` field matches the header value.

**Conversation messages:**

| Type                   | Description                                   |
| ---------------------- | --------------------------------------------- |
| `conversation.message` | Text content in an ongoing conversation       |
| `conversation.join`    | Agent joining a conversation (M:N topologies) |
| `conversation.leave`   | Agent leaving a conversation                  |

**Tool and offering invocation:**

| Type                | Description                                       |
| ------------------- | ------------------------------------------------- |
| `offering.request`  | Invoking another agent's offering with parameters |
| `offering.response` | Result of an offering invocation                  |
| `offering.error`    | Offering invocation failed                        |
| `offering.discover` | Querying an agent's available offerings           |
| `offering.catalog`  | Response to a discovery query                     |

**Payment (x402 flow):**

| Type               | Description                                          |
| ------------------ | ---------------------------------------------------- |
| `payment.required` | Payment required before proceeding (HTTP 402 analog) |
| `payment.receipt`  | Proof of payment                                     |
| `payment.verified` | Payment verified, proceeding                         |

**Approval:**

| Type               | Description                                        |
| ------------------ | -------------------------------------------------- |
| `approval.request` | Agent requesting human approval for a gated action |
| `approval.granted` | Approval given                                     |
| `approval.denied`  | Approval denied, with optional reason              |

**System:**

| Type                        | Description                     |
| --------------------------- | ------------------------------- |
| `system.health`             | Health report                   |
| `system.register`           | Agent announcing presence       |
| `system.deregister`         | Agent shutting down             |
| `system.credential.refresh` | Credential refresh notification |

Each structured payload type has a defined JSON schema. The schema version is carried in the `Interchange-Schema-Version` header for forward compatibility. Receivers that encounter an unknown schema version or unknown type treat the message as opaque data and surface it to the plugin for handling.

### Payload Structure

Conversation messages (`conversation.message`, `conversation.join`, `conversation.leave`) have `text/plain` bodies. No JSON envelope, no schema. The message is the text.

Structured messages use `application/vnd.interchange+json` with a common envelope:

```json
{
  "type": "offering.request",
  "version": "1",
  "body": { ... }
}
```

The `type` field matches the `Interchange-Type` header. The `version` field matches the `Interchange-Schema-Version` header. The `body` field varies by type. Examples:

**offering.request:**

```json
{
  "type": "offering.request",
  "version": "1",
  "body": {
    "offeringId": "code-review",
    "parameters": {
      "repository": "https://github.com/example/repo",
      "branch": "feature/auth",
      "scope": "security"
    }
  }
}
```

**offering.response:**

```json
{
  "type": "offering.response",
  "version": "1",
  "body": {
    "status": "complete",
    "result": {
      "findings": [
        {
          "severity": "high",
          "file": "auth.ts",
          "line": 42,
          "message": "Race condition in token refresh"
        }
      ]
    }
  }
}
```

**payment.required:**

```json
{
  "type": "payment.required",
  "version": "1",
  "body": {
    "amount": "0.50",
    "currency": "USD",
    "methods": ["faremeter"],
    "description": "Code review of feature/auth branch",
    "payTo": "code-reviewer@tenant.interchange.network",
    "expiresAt": "2026-04-13T19:30:00Z"
  }
}
```

**payment.receipt:**

```json
{
  "type": "payment.receipt",
  "version": "1",
  "body": {
    "transactionId": "txn-789",
    "amount": "0.50",
    "currency": "USD",
    "method": "faremeter",
    "paidBy": "requesting-agent@tenant.interchange.network",
    "paidTo": "code-reviewer@tenant.interchange.network"
  }
}
```

**payment.verified:**

```json
{
  "type": "payment.verified",
  "version": "1",
  "body": {
    "transactionId": "txn-789",
    "status": "confirmed"
  }
}
```

## Threading and Conversations

Conversations are threaded using RFC 5322 `In-Reply-To` and `References` headers. Every reply carries the parent's `Message-ID` in `In-Reply-To` and the full ancestry chain in `References`.

The `References` header is constructed per RFC 5322: the parent's `References` value (if any) followed by the parent's `Message-ID`. This creates a traversable ancestry chain. The first entry in `References` is the root of the conversation.

```
Message A: Message-ID: <a@example>

Message B (reply to A):
  In-Reply-To: <a@example>
  References: <a@example>

Message C (reply to B):
  In-Reply-To: <b@example>
  References: <a@example> <b@example>

Message D (reply to A, branching):
  In-Reply-To: <a@example>
  References: <a@example>
```

This naturally represents tree-shaped conversations. Linear conversations are a degenerate case.

### Thread Retrieval

IMAP servers that support the THREAD extension (RFC 5256) reconstruct conversation trees server-side using the REFERENCES algorithm, which builds parent-child relationships from `In-Reply-To` and `References` headers. For IMAP servers without THREAD support, the client reconstructs threads from fetched `References` headers.

### Correlation

The `Interchange-Correlation-ID` header links asynchronous request-response pairs. When an agent sends an `offering.request`, it assigns a cryptographically random correlation ID (UUID v4). The responding agent copies the correlation ID into its `offering.response`. The reactor matches the response to the pending request.

Correlation IDs are distinct from Message-IDs and References. A correlation ID links semantic request-response pairs at the Interchange protocol level. Message-IDs and References provide SMTP-level threading. An offering request and its response are both part of the same conversation (via References) and linked as a logical pair (via correlation ID).

### Correlation Security

The reactor uses a pluggable correlation validator (see INFERENCE.md, Correlation). For message correlation, the validator enforces three conditions before accepting a match:

1. The inbound message's `Interchange-Correlation-ID` matches a registered pending correlation.
2. The inbound message's `From` address matches the expected responder recorded at registration time.
3. The inbound message's PGP/MIME signature is valid and was produced by the expected responder's key.

All three are required. A message that matches the correlation ID but fails sender or signature verification is rejected by the validator and delivered to the plugin as a regular `message.received` event. This prevents a third party from injecting forged responses with guessed or intercepted correlation IDs. The cryptographic randomness of the IDs makes guessing infeasible; the sender and signature checks make interception-based forgery infeasible.

## Cryptographic Signing

Every outbound message is signed with the sending agent's Ed25519 private key. The signature is carried as a PGP/MIME detached signature (RFC 3156) in the `multipart/signed` envelope.

### Signing Process

1. The signed content is assembled — `text/plain` for conversation messages, `multipart/mixed` for structured messages
2. Content is canonicalized: CRLF line endings, trailing whitespace removed, 7-bit encoding applied (base64 for binary parts, quoted-printable for 8-bit text)
3. The payload is hashed (SHA-512, as required by Ed25519's internal construction)
4. The hash is signed with the agent's Ed25519 private key
5. The signature is encoded as an `application/pgp-signature` part
6. The payload and signature are wrapped in `multipart/signed`

### Verification Process

1. Recipient extracts the signed content (IMAP `BODY[1]`) and the signature (`BODY[2]`)
2. Payload is canonicalized using the same rules
3. Signature is verified against the sender's Ed25519 public key
4. Public key is resolved from: the control plane's published keys, DNS OPENPGPKEY records (RFC 7929), or a previously established key exchange

### Key Distribution

Agent public keys are published through the control plane and included in agent discovery metadata. For cross-tenant messages, public keys can additionally be published via DNS DANE/OPENPGPKEY records, providing a federated key distribution mechanism that does not require the receiving tenant to trust the sending tenant's control plane.

The control plane also stores agent public keys for session management. When a harness reconnects after a restart, it proves ownership of agent addresses by signing a challenge with the agent's private key. The control plane verifies the signature against the stored public key before re-establishing the agent's sessions.

## Inbox Management (IMAP Semantics)

The agent's IMAP inbox is not just a delivery endpoint. It is a queryable, stateful message store. The harness uses IMAP semantics to manage the agent's message lifecycle.

### Mailbox Structure

Every agent has a standard set of mailboxes:

| Mailbox   | Role (RFC 9051) | Purpose                                   |
| --------- | --------------- | ----------------------------------------- |
| `INBOX`   | `\Inbox`        | Incoming messages                         |
| `Sent`    | `\Sent`         | Copies of outbound messages               |
| `Drafts`  | `\Drafts`       | Messages under composition                |
| `Archive` | `\Archive`      | Processed messages retained for history   |
| `Trash`   | `\Trash`        | Deleted messages before permanent removal |

Agents may create additional mailboxes for organizational purposes, though the default structure is sufficient for most workflows. Thread-based organization (via IMAP THREAD) is preferred over per-conversation folders.

### Message Flags and Keywords

Standard IMAP system flags:

| Flag        | Semantics                          |
| ----------- | ---------------------------------- |
| `\Seen`     | Message has been read by the agent |
| `\Answered` | Message has been replied to        |
| `\Flagged`  | Message is flagged for attention   |
| `\Deleted`  | Message is marked for expulsion    |
| `\Draft`    | Message is a draft                 |

Interchange-specific keywords (IMAP permits arbitrary keywords as flags):

| Keyword          | Semantics                                                                         |
| ---------------- | --------------------------------------------------------------------------------- |
| `$Processed`     | Agent has fully processed this message (tool invocation complete, response sent)  |
| `$Pending`       | Message generated a pending operation; awaiting correlated response               |
| `$Correlated`    | This message is a correlated response to a pending request                        |
| `$GateBlocked`   | Message triggered a gate (approval, payment, credential) that has not yet cleared |
| `$SystemMessage` | Message is a system-level signal, not a conversation message                      |

Keywords enable efficient search. An agent checking for unprocessed messages searches `UNKEYWORD $Processed`. An agent looking for pending operations searches `KEYWORD $Pending UNKEYWORD $Correlated`.

### Search

IMAP SEARCH (RFC 9051) provides server-side message filtering with criteria for addresses, dates, flags/keywords, content, size, and boolean composition. The transport interface's `SearchQuery` type maps these to a structured object (see Transport Interface below). The full IMAP SEARCH grammar is defined in RFC 9051 Section 6.4.4.

The `HEADER` search key is critical for Interchange: it enables filtering by Interchange-specific headers without parsing message bodies. Interchange-specific search patterns:

Find unprocessed conversation messages:

```
UNKEYWORD $Processed HEADER Interchange-Type conversation.message
```

Find pending offering requests:

```
KEYWORD $Pending UNKEYWORD $Correlated HEADER Interchange-Type offering.request
```

Find messages from a specific agent since a date:

```
FROM agent-x@tenant.interchange.network SINCE 10-Apr-2026
```

Find correlated response for a specific request:

```
HEADER Interchange-Correlation-ID abc123 HEADER Interchange-Type offering.response
```

### Partial Fetch

IMAP FETCH with section specifiers enables retrieving specific parts of a message without downloading the entire thing. This is critical for messages with large attachments.

| Fetch specifier                                               | What it retrieves                                                                |
| ------------------------------------------------------------- | -------------------------------------------------------------------------------- |
| `BODY[HEADER]`                                                | All message headers                                                              |
| `BODY[HEADER.FIELDS (From To Subject Date Interchange-Type)]` | Specific headers only                                                            |
| `BODY[1]`                                                     | Signed content (`text/plain` for conversation, `multipart/mixed` for structured) |
| `BODY[1.1]`                                                   | The `application/vnd.interchange+json` payload (structured messages only)        |
| `BODY[2]`                                                     | The PGP signature                                                                |
| `BODYSTRUCTURE`                                               | MIME structure metadata (types, sizes, part hierarchy) without any content       |

The `BODYSTRUCTURE` fetch is particularly useful. It returns the MIME tree structure — content types, sizes, dispositions, parameters — for every part of the message without transferring any content. An agent can examine the structure to decide which parts to fetch.

`BODY.PEEK[section]` retrieves content without setting the `\Seen` flag. Agents performing automated processing should use PEEK to avoid prematurely marking messages as read.

### Real-Time Notifications

IMAP IDLE (RFC 9051, incorporated from RFC 2177) provides push notification for new messages. The client enters IDLE state and the server sends untagged `EXISTS` responses when new messages arrive. The client sends `DONE` to exit IDLE and resume normal command processing.

For the harness, the IMAP connection enters IDLE when the reactor is waiting for events. New message delivery triggers an `EXISTS` notification, which the harness translates into a `message.received` event for the reactor.

### Modification Sequences (CONDSTORE)

IMAP4rev2 incorporates CONDSTORE. Every message has a modification sequence number (MODSEQ) that increments when the message's flags or metadata change. The HIGHESTMODSEQ value tracks the mailbox-level high water mark.

Agents can use `CHANGEDSINCE` to efficiently detect changes since their last check:

```
FETCH 1:* (FLAGS) (CHANGEDSINCE 12345)
```

This returns only messages whose flags changed since MODSEQ 12345, enabling efficient incremental synchronization without scanning the entire mailbox.

### Quick Resynchronization (QRESYNC)

IMAP4rev2 incorporates QRESYNC. When reconnecting after a disconnect, the client provides its last known `UIDVALIDITY` and `UIDNEXT` values along with any known UIDs. The server responds with `VANISHED` (UIDs that were expunged) and `FETCH` (messages that changed), enabling the client to synchronize without re-fetching the entire mailbox.

For agents that suspend and resume, QRESYNC provides efficient inbox reconciliation on restart.

## Message Topologies

SMTP naturally supports the topologies defined in the architecture:

**1:1 (Direct):** Standard SMTP delivery from one address to another. Each message has one `From` and one `To`.

**1:N (Broadcast):** Multiple `To` or `Cc` recipients. The sender addresses multiple agents. Each recipient receives an independent copy. This is suitable for announcements, status broadcasts, or fan-out patterns.

**M:N (Collaborative):** SMTP distribution lists. Multiple agents post to a shared list address and all subscribers receive all messages. The list address acts as the conversation identity. Replies go to the list, not individual senders. IMAP THREAD REFERENCES reconstructs the conversation structure from the full set of list messages.

Messages sent to a list carry a `List-ID` header (RFC 2919) identifying the list. This enables agents to filter and organize list traffic via IMAP SEARCH (`HEADER List-ID <list-id>`).

Distribution list management (subscribe, unsubscribe, moderation) is handled by the control plane, which configures the underlying mail infrastructure. The transport interface exposes list operations:

```
createList(address: string, name: string): Promise<ListInfo>
listMembers(address: string): Promise<string[]>
subscribe(listAddress: string, agentAddress: string): Promise<void>
unsubscribe(listAddress: string, agentAddress: string): Promise<void>
```

`ListInfo` includes: address, name, member count, creation date. When an agent joins a conversation via `conversation.join`, the harness subscribes the agent to the corresponding list. When it leaves via `conversation.leave`, the harness unsubscribes.

## Transport Interface

The transport interface abstracts SMTP and IMAP behind a TypeScript API. Implementations provide the actual protocol handling; the harness uses the interface without knowing whether messages are traveling over the network or through memory.

The interface splits into three concerns: outbound delivery, inbox management, and real-time notification.

### Outbound

```
send(message: OutboundMessage): Promise<SendReceipt>
```

Composes the MIME structure (signed multipart with structured payload), delivers via SMTP, and returns a receipt containing the assigned Message-ID and delivery status.

The `OutboundMessage` carries:

- Recipient address(es) and topology (to, cc)
- The structured Interchange payload (type + body)
- Optional text summary
- Optional attachments (content type, filename, data)
- Threading context (in-reply-to Message-ID, correlation ID)
- Session and tenant context (from Interchange headers)

The transport implementation handles MIME assembly, PGP signing (using the CryptoProvider), Content-Transfer-Encoding, SMTP submission, and appending a copy to the sender's `Sent` mailbox (IMAP APPEND). The harness provides the semantic content; the transport handles the wire format.

**Append:**

```
append(mailbox: string, message: InboundMessage, flags?: string[]): Promise<MessageRef>
```

Appends a message to a mailbox (IMAP APPEND, RFC 9051 Section 6.3.12). Used internally by `send()` to populate the `Sent` mailbox. Also available directly for the harness to inject synthetic messages (resolution messages, system notifications) into an agent's mailbox.

### Inbox

The inbox interface captures IMAP semantics:

**Mailbox management:**

```
listMailboxes(): Promise<Mailbox[]>
createMailbox(name: string): Promise<Mailbox>
deleteMailbox(name: string): Promise<void>
getMailboxStatus(name: string): Promise<MailboxStatus>
```

`MailboxStatus` includes: total messages, unseen count, recent count, UIDNEXT, UIDVALIDITY, HIGHESTMODSEQ.

**Message search:**

```
search(mailbox: string, query: SearchQuery): Promise<MessageRef[]>
```

`SearchQuery` maps the IMAP SEARCH grammar to a structured object:

- Address filters: `from`, `to`, `cc`, `bcc` (substring match)
- Header filter: `header` as `{ field: string, contains: string }` for Interchange-specific headers
- Date filters: `before`, `after`, `on` (delivery date); `sentBefore`, `sentAfter`, `sentOn` (origination date)
- Flag filters: `hasFlags`, `missingFlags` (system flags and keywords)
- Content filters: `body` (body text), `text` (headers + body)
- Size filters: `largerThan`, `smallerThan` (octets)
- Boolean: `and`, `or`, `not` (recursive composition)

`MessageRef` is an opaque reference (UID + mailbox) that can be passed to fetch, flag, and move operations.

**Thread retrieval:**

```
thread(mailbox: string, algorithm: "references" | "orderedsubject", query?: SearchQuery): Promise<Thread[]>
```

Returns conversations as tree structures. Each `Thread` node carries a `MessageRef` and an array of child `Thread` nodes. The `references` algorithm (RFC 5256) builds trees from `In-Reply-To` and `References` headers. The optional `query` parameter restricts threading to messages matching the search criteria.

**Message fetch:**

```
fetchHeaders(ref: MessageRef): Promise<MessageHeaders>
fetchStructure(ref: MessageRef): Promise<BodyStructure>
fetchPart(ref: MessageRef, partPath: string): Promise<MessagePart>
fetchFull(ref: MessageRef): Promise<InboundMessage>
```

`fetchHeaders` retrieves only headers (IMAP `BODY.PEEK[HEADER]`). Fast, does not mark as read.

`fetchStructure` retrieves the MIME tree metadata (IMAP `BODYSTRUCTURE`): content types, sizes, dispositions, parameters for every part. No content transferred.

`fetchPart` retrieves a single MIME part by dot-separated path (IMAP `BODY.PEEK[path]`). Used to fetch just the JSON payload (`1.1`) or just an attachment (`1.3`) without downloading the entire message.

`fetchFull` retrieves the complete message, parses the MIME structure, verifies the PGP signature, and returns a fully parsed `InboundMessage` with structured payload, headers, and attachments. The returned `InboundMessage` includes a `signatureStatus` field: `"valid"` (signature verified against sender's public key), `"invalid"` (signature check failed — tampering or wrong key), `"unknown"` (public key not available for verification), or `"missing"` (message was not signed). The harness decides policy based on this field — cross-tenant messages with `"invalid"` or `"missing"` status should be rejected; intra-tenant messages may be accepted with reduced trust depending on tenant policy.

**Flag management:**

```
setFlags(ref: MessageRef, flags: string[]): Promise<void>
clearFlags(ref: MessageRef, flags: string[]): Promise<void>
```

Sets or clears system flags and custom keywords. Used by the harness to track processing state (`$Processed`, `$Pending`, `$Correlated`).

**Message organization:**

```
move(ref: MessageRef, toMailbox: string): Promise<void>
copy(ref: MessageRef, toMailbox: string): Promise<void>
expunge(mailbox: string): Promise<void>
```

`move` relocates a message (IMAP MOVE, RFC 9051). `expunge` permanently removes messages flagged `\Deleted`.

### Real-Time Notification

```
watch(mailbox: string, callback: (event: MailboxEvent) => void): Unsubscribe
```

Provides IMAP IDLE semantics. The transport monitors the specified mailbox and invokes the callback when:

- A new message arrives (`exists` event with the new message UID and headers)
- A message's flags change (`flagsChanged` event with the UID and new flags)
- A message is expunged (`expunged` event with the UID)

The `exists` event includes the message headers (fetched internally by the transport via `BODY.PEEK[HEADER]` on notification). This avoids a round-trip from the harness to read headers for routing decisions — the transport pays this cost once per delivery.

The callback receives typed events. The harness translates `exists` events into `message.received` reactor events.

Callbacks are always invoked asynchronously, even in the in-memory transport. Delivery during a `send()` call must not invoke the recipient's callback synchronously on the sender's call stack. This preserves the async delivery semantics of real IMAP IDLE and prevents re-entrant transport operations.

**IMAP IDLE constraint:** Standard IMAP IDLE monitors only the currently selected mailbox. Watching multiple mailboxes requires multiple IMAP connections. The `message-smtp` transport implementation manages a connection pool internally. The interface permits multiple concurrent `watch()` calls — the implementation is responsible for the underlying connection management.

### Synchronization

```
sync(mailbox: string, knownState: SyncState): Promise<SyncResult>
```

Efficient reconnection using QRESYNC semantics. The harness provides its last known state (UIDVALIDITY, UIDNEXT, HIGHESTMODSEQ, known UIDs). The transport returns:

- `vanished`: UIDs that were expunged since last sync
- `changed`: messages whose flags changed since last sync
- `new`: messages that arrived since last sync

If UIDVALIDITY has changed (mailbox was recreated), the transport signals a full resync is required.

## Messaging Tools

The agent interacts with the message transport through tools exposed by the harness. These tools are what the inference layer presents to the model. They map to the transport interface operations.

### Tool Definitions

**message.send** — Send a message to one or more recipients.

Parameters:

- `to`: recipient address or array of addresses
- `subject`: conversation topic (optional, carried forward in replies)
- `content`: text content of the message (for `conversation.message` type)
- `payload`: structured payload object (optional — for non-conversation types, replaces `content` with the full `body` object for the given `type`)
- `inReplyTo`: Message-ID being replied to (optional — sets In-Reply-To and extends References chain by fetching the parent's References header)
- `correlationId`: links this message to a pending request (optional)
- `type`: Interchange payload type (default: `conversation.message`)
- `attachments`: array of `{ name, contentType, data }` (optional)

When `type` is a conversation type, the `content` string becomes the `text/plain` message body. For structured types, the `payload` object becomes the `body` field of the `application/vnd.interchange+json` part. Providing both `content` and `payload` is an error.

Returns on success: `{ messageId: string }` if delivery is fire-and-forget, or `{ messageId: string, status: "pending", correlationId: string }` if the message expects a correlated response.

Returns on error: `{ error: string, code: string }`. Error codes: `invalid_address` (unresolvable recipient), `invalid_type` (unknown payload type), `too_large` (message exceeds size limit), `send_failed` (SMTP submission rejected).

**message.reply** — Reply to a specific message. Convenience wrapper around `message.send` that automatically sets `inReplyTo` and extends the `References` chain from the parent message.

Parameters:

- `ref`: reference to the message being replied to (from search or read results)
- `content`: text content (for conversation replies)
- `payload`: structured payload object (optional, for non-conversation reply types)
- `type`: Interchange payload type (default: `conversation.message` — use `offering.response` when replying to an offering request)
- `attachments`: optional

Returns: same as `message.send`

**message.search** — Search the inbox.

Parameters:

- `mailbox`: mailbox to search (default: `INBOX`)
- `query`: search criteria (structured object matching the SearchQuery type)
- `limit`: maximum results (default: 20)

Returns: array of message summaries (message ref, headers, payload type, preview text, flags, timestamp). Not full content.

Returns on error: `{ error: string, code: string }`. Error codes: `invalid_mailbox` (mailbox does not exist), `invalid_query` (malformed search criteria).

**message.read** — Read a specific message.

Parameters:

- `ref`: message reference (from search results)
- `parts`: which parts to fetch — `"headers"`, `"payload"`, `"full"`, or a specific MIME part path like `"1.3"` (default: `"payload"`)

Returns: the requested content. For `"payload"`, returns the parsed `application/vnd.interchange+json` object. For `"full"`, returns the complete parsed message including signature status.

Returns on error: `{ error: string, code: string }`. Error codes: `not_found` (message no longer exists), `invalid_part` (requested MIME part does not exist).

**message.threads** — Get conversation threads.

Parameters:

- `mailbox`: mailbox to thread (default: `INBOX`)
- `query`: optional search criteria to filter which messages are threaded
- `limit`: maximum threads (default: 10)

Returns: array of thread trees, each with message summaries and child threads.

**message.flag** — Set or clear flags on a message.

Parameters:

- `ref`: message reference
- `set`: flags to add (system flags or custom keywords)
- `clear`: flags to remove

Returns: `{ ok: true }`

Returns on error: `{ error: string, code: string }`. Error codes: `not_found`, `invalid_flag` (flag name not permitted by server).

**message.move** — Move a message to a different mailbox.

Parameters:

- `ref`: message reference
- `to`: destination mailbox name

Returns: `{ ok: true }`

Returns on error: `{ error: string, code: string }`. Error codes: `not_found`, `invalid_mailbox`.

**message.wait** — Block until a message matching a query arrives.

Parameters:

- `query`: search criteria (same shape as `message.search` query — e.g. `{ from: "agent@..." }`)
- `timeout`: maximum seconds to wait (default: 120)
- `mailbox`: mailbox to watch (default: `INBOX`)

Checks for existing matches first via `search`. If none found, subscribes to the transport's `watch` mechanism and blocks until a matching `exists` event fires or the timeout expires. The tool respects the reactor's abort signal.

Returns on success: `{ ref, from, subject, content }` — the matched message's reference, sender, subject, and text content.

Returns on error: `{ error: string, code: string }`. Error codes: `timeout` (no matching message arrived within the deadline), `aborted` (reactor shut down while waiting).

Use this instead of polling `message.search` in a loop. The blocking behavior is transparent to the reactor — the tool's promise simply takes longer to resolve, and the agent naturally idles until it does.

### Offering Tools

Offering tools are convenience wrappers over the message transport for the common pattern of invoking another agent's offering and receiving the result. They construct the correct payload types and handle correlation.

**offering.invoke** — Invoke an offering on another agent.

Parameters:

- `to`: target agent address
- `offeringId`: the offering to invoke
- `parameters`: offering-specific parameters (object)

Internally sends an `offering.request` message with the correct payload structure and `Interchange-Offering-ID` header. Always returns a pending marker with a correlation ID, since offering invocations are inherently asynchronous.

Returns: `{ messageId: string, status: "pending", correlationId: string }`

Returns on error: `{ error: string, code: string }`. Error codes: `invalid_address`, `send_failed`.

**offering.discover** — Query another agent's available offerings.

Parameters:

- `to`: target agent address

Sends an `offering.discover` message. Returns a pending marker; the catalog arrives as a correlated `offering.catalog` response.

Returns: `{ messageId: string, status: "pending", correlationId: string }`

### Pending Marker Pattern

When `message.send` is used for an offering request (type `offering.request`), the tool returns a pending marker:

```json
{
  "messageId": "<abc@tenant.interchange.network>",
  "status": "pending",
  "correlationId": "req-abc123"
}
```

The reactor registers the correlation ID and the expected responder (the `to` address from the sent message) in its async state. The plugin sees the pending marker and decides the wait strategy: suspend at a gate, continue working, or fork a child to wait. See INFERENCE.md (Tool Execution Semantics) for the full pattern.

When the response arrives, the reactor's message correlation validator checks the three-condition match (see Correlation Security). On success, the reactor clears the gate, injects the resolution, and emits a `message.correlated` event.

## In-Memory Transport

For development, testing, and the initial prototype, an in-memory transport implements the full interface without network I/O. Messages are routed through memory within a single process.

The in-memory transport maintains:

- A map of agent addresses to mailbox stores (each store is a map of mailbox name to message array)
- A UID counter per mailbox
- A MODSEQ counter per mailbox
- A set of watch callbacks per mailbox

`send()` assembles the message structure, signs it using the provided CryptoProvider, assigns a Message-ID, and delivers it directly to the recipient's INBOX by appending to the array and incrementing the UID counter. Watch callbacks fire synchronously on delivery.

Search, fetch, thread, flag, and move operations are array operations over the in-memory store. THREAD REFERENCES is implemented as the RFC 5256 algorithm over the stored messages. BODYSTRUCTURE is computed from the message's MIME metadata. Partial fetch returns the requested part from the parsed structure.

The in-memory transport is not a mock. It implements the full interface with correct semantics — UID ordering, MODSEQ tracking, flag persistence, thread construction. The only difference from a real SMTP/IMAP transport is that messages do not traverse a network.
