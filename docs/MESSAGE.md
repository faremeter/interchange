# Faremeter Interchange

_Message Transport_

Mail is the first-class communication primitive in Interchange. The hub stores raw MIME bytes at routing time. Clients interact with mail through the REST API, which serves parsed views following the JMAP Email object model (RFC 8621). Attachments are referenced by blob IDs and fetched separately via `GET /blobs/:blobId`. Direction is always from the agent's perspective: mail arriving at an agent is `inbound`; mail the agent sends is `outbound`.

The wire protocol between agents is SMTP and IMAP. Agents send messages via SMTP and manage their inbox via IMAP. The transport layer is abstracted behind an interface that captures the full semantics of both protocols, allowing implementations to range from real SMTP/IMAP servers to in-process stubs that route messages through memory.

This document specifies the message format, the IMAP inbox model, and the transport interface. It is the contract between the harness (which uses the interface) and the transport implementations (which provide it).

## Agent Addressing

Every agent has an SMTP address as its network identity:

```
run_xxxxx@tenant.interchange.network
```

The local part identifies the agent within a tenant. The domain identifies the tenant. Tenant boundaries map directly to SMTP domains, providing natural isolation and federation semantics. DNS MX records route messages to the correct infrastructure. Each agent launched from a definition receives its own address, allowing multiple concurrent agents from the same definition.

Address format follows RFC 5322 addr-spec: `local-part "@" domain`. The local part is a dot-atom built from the RFC 5322 `atext` characters — letters, digits, dots, and the punctuation `atext` permits, which includes the underscore. No quoted strings. Every agent local part carries the `run_` prefix (see `parseRunAddress` in `@intx/types`). Not every Interchange address is an agent address: a user principal is addressed as `<refId>@<domain>`. Addresses are assigned at agent launch time and are unique within a tenant.

## Message Format

Every Interchange message is a PGP/MIME signed (RFC 3156) multipart message. The signature provides content provenance — recipients verify that the message was produced by the claimed sender's Ed25519 key.

### MIME Structure

The outer layer is always `multipart/signed` per RFC 3156. The signed content is the first part; the detached PGP signature is the second part, computed after canonicalization (trailing whitespace removed, line endings normalized to CRLF, content constrained to 7-bit).

The signed content varies by message type. Conversation messages use `multipart/mixed` with a `text/plain` part plus zero or more attachment parts — they are still ordinary signed emails (a mail client renders the single `text/plain` part the same as a bare body). Structured messages (offerings, payments, approvals, system) use `multipart/mixed` with an `application/vnd.interchange+json` part carrying machine-readable data.

**Conversation messages:**

The shape is `multipart/mixed` unconditionally — every conversation message has the same structure regardless of attachment count.

```
multipart/signed; protocol="application/pgp-signature"; micalg=pgp-sha512
├── multipart/mixed
│   ├── text/plain                          [the message]
│   └── [attachment parts] (zero or more)   [images, audio, video, documents]
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

The `Interchange-Type` header determines which shape to expect. Conversation types (`conversation.message`, `conversation.join`, `conversation.leave`) use the conversation form (text in part `1.1`). All other types use the structured form.

### Part Addressing

IMAP FETCH addresses MIME parts by position using dot-separated numeric paths (RFC 9051). Part paths differ between the two MIME shapes.

**Conversation messages:**

| IMAP Part | Content                                       |
| --------- | --------------------------------------------- |
| `1`       | The `multipart/mixed` payload (all sub-parts) |
| `1.1`     | The `text/plain` message body                 |
| `1.2+`    | Attachments (if present)                      |
| `2`       | The `application/pgp-signature`               |

**Structured messages:**

| IMAP Part | Content                                       |
| --------- | --------------------------------------------- |
| `1`       | The `multipart/mixed` payload (all sub-parts) |
| `1.1`     | The `application/vnd.interchange+json` part   |
| `1.2`     | The `text/plain` summary (if present)         |
| `1.3+`    | Attachments (if present)                      |
| `2`       | The `application/pgp-signature`               |

For conversation messages, `BODY[1.1]` fetches the text without downloading attachments. For structured messages, `BODY[1.1]` fetches just the JSON payload. In both cases, `BODY[2]` fetches the signature for verification.

### Headers

Every message carries standard RFC 5322 headers plus Interchange-specific headers.

**Standard headers (RFC 5322):**

| Header         | Usage                                                                                         |
| -------------- | --------------------------------------------------------------------------------------------- |
| `From`         | Sender's run address                                                                          |
| `To`           | Recipient address(es). Multiple recipients for 1:N broadcast.                                 |
| `Cc`           | Additional recipients (visible to all)                                                        |
| `Date`         | Origination timestamp (RFC 5322 date-time format)                                             |
| `Message-ID`   | Unique identifier: `<uuid@tenant.interchange.network>`                                        |
| `In-Reply-To`  | Message-ID of the parent message (for threading)                                              |
| `References`   | Ancestry chain                                                                                |
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

The `Interchange-Type` header identifies every message's type for routing without body parsing. Conversation types carry their text in the `text/plain` part of a `multipart/mixed` body. All other types use `application/vnd.interchange+json` with a JSON object whose `type` field matches the header value.

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

The schema version is carried in the `Interchange-Schema-Version` header for forward compatibility. Receivers that encounter an unknown schema version or unknown type treat the message as opaque data and surface it to the plugin for handling.

> **Planned / Not Yet Implemented.** Per-type JSON schemas do not exist today. One validator covers every structured type: it checks that `type` is one of the values listed above and that `version` is a string, and accepts `body` as an open `Record<string, unknown>` whatever the type. The per-type bodies shown in Payload Structure below are the intended design, not a validated contract — nothing rejects a `payment.required` body that omits `amount`.

### Payload Structure

Conversation messages (`conversation.message`, `conversation.join`, `conversation.leave`) carry the text in the `text/plain` part of a `multipart/mixed` body, optionally followed by binary attachment parts. No JSON envelope, no schema for the text — the message is the text, and any attachments ride alongside it as standard MIME parts.

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

Conversations are threaded using RFC 5322 `In-Reply-To` and `References` headers. Every reply carries the parent's `Message-ID` in `In-Reply-To`. `mail.send` with an `inReplyTo` argument never reads the parent, so its `References` chain names that one parent alone.

The `References` header is constructed per RFC 5322: the parent's `References` value (if any) followed by the parent's `Message-ID`. This creates a traversable ancestry chain.

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

The reactor uses a pluggable correlation validator (see INFERENCE.md, Correlation). For message correlation, three conditions are intended to hold before a match is accepted:

1. The inbound message's `Interchange-Correlation-ID` matches a registered pending correlation.
2. The inbound message's `From` address matches the responder the validator expects for that correlation.
3. The inbound message's PGP/MIME signature is valid and was produced by that responder's key.

All three are required under that design. A message that matches the correlation ID but fails sender or signature verification is rejected by the validator and delivered to the plugin as a regular `message.received` event.

> **Planned / Not Yet Implemented.** Only condition 1 is enforced today, and the reactor enforces it itself by looking the correlation ID up in its registry. `CorrelationValidator` in `@intx/inference` is an interface and an optional reactor option with no production implementation behind it: nothing in the shipped composition supplies one, so the sender and signature checks in conditions 2 and 3 do not run. A message carrying a registered correlation ID is accepted on that ID alone.

**Treat a correlation ID as a capability: anyone who learns one can answer it.** With no validator wired there is no check on `From`, no check on the signature, and no check that the responder is the party the correlation was issued to. Resolving a correlation is not inert bookkeeping — on the approval rail it clears the gate a tool call suspended on and grants that call a one-shot authorization bypass.

The guarantee that remains is scope: a correlation is resolved only by a message bearing an ID that same reactor registered, and the registry is per-reactor closure state. Whether the ID can be guessed depends on who minted it — the authz `ask` flow mints `crypto.randomUUID()` values, while an ID on a tool's pending marker is whatever the tool chose.

Two ways an ID leaves the agent that holds it:

- `to` accepts an array, so every recipient of a multi-recipient message learns the ID that message carries.
- `mail_read` returns the whole header set for `parts: "headers"` and `parts: "full"`, `Interchange-Correlation-ID` included. The default `parts: "payload"` does not expose it.

The hub and sidecar stack is not exposed: the workflow host's projection from decoded mail to the step agent drops the header, so no inbound mail reaches correlation resolution there. `@intx/harness` is exposed. It is the one composition in this tree that feeds `fetchFull` output straight into `agent.deliver`, with no projection between them, so a mail-supplied correlation ID reaches correlation resolution on the header alone. Closing that needs either a correlation validator on the reactor or a delivery-side projection that drops the header, as the workflow host's does.

## Cryptographic Signing

Every outbound message is signed with the sending agent's Ed25519 private key. The signature is carried as a PGP/MIME detached signature (RFC 3156) in the `multipart/signed` envelope.

### Signing Process

1. The signed content is assembled — `multipart/mixed` for both conversation and structured messages
2. Content is canonicalized: CRLF line endings, trailing whitespace removed, every attachment part base64-encoded whatever its content type
3. The payload is hashed (SHA-512, as required by Ed25519's internal construction)
4. The hash is signed with the agent's Ed25519 private key
5. The signature is encoded as an `application/pgp-signature` part
6. The payload and signature are wrapped in `multipart/signed`

> **Planned / Not Yet Implemented.** Quoted-printable encoding of 8-bit text is not produced. `@intx/mime` decodes quoted-printable on an inbound part, but the assembler has no encoder: it labels every text part `7bit` and writes the UTF-8 bytes as they are. A body carrying characters outside US-ASCII therefore ships mislabelled.

### Verification Process

1. Recipient extracts the signed content (IMAP `BODY[1]`) and the signature (`BODY[2]`)
2. Payload is canonicalized using the same rules
3. Signature is verified against the sender's Ed25519 public key
4. Public key is resolved from the control plane's published keys. DNS OPENPGPKEY records (RFC 7929) and a previously established key exchange are the intended additional sources; neither is implemented (see below)

### Key Distribution

Agent public keys are published through the control plane and included in agent discovery metadata.

> **Planned / Not Yet Implemented.** DNS DANE/OPENPGPKEY (RFC 7929) key distribution does not exist. Nothing in the tree performs a DNS lookup, and `@intx/crypto` has no transferable public key format to publish in such a record: its OpenPGP support covers detached signature packets and signature armor only, with no public key packet. Key resolution goes to the control plane in every case (see `resolveSenderKey` in `@intx/db`). A previously established key exchange is likewise not a source any code consults. A federated path that does not require the receiving tenant to trust the sending tenant's control plane is the intended design, not current behavior.

The control plane also stores agent public keys for content and commit provenance. Sidecar reconnection uses a separate allocation-scoped bearer credential that resolves to one deployment address and generation; the public key is not routing authority.

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
| `$FetchFailed`   | A fetch of this message threw; the harness left it undelivered in the INBOX       |

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

| Fetch specifier                                               | What it retrieves                                                                               |
| ------------------------------------------------------------- | ----------------------------------------------------------------------------------------------- |
| `BODY[HEADER]`                                                | All message headers                                                                             |
| `BODY[HEADER.FIELDS (From To Subject Date Interchange-Type)]` | Specific headers only                                                                           |
| `BODY[1]`                                                     | Signed content (`multipart/mixed` for both conversation and structured)                         |
| `BODY[1.1]`                                                   | The `text/plain` body (conversation) or `application/vnd.interchange+json` payload (structured) |
| `BODY[2]`                                                     | The PGP signature                                                                               |
| `BODYSTRUCTURE`                                               | MIME structure metadata (types, sizes, part hierarchy) without any content                      |

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
subscribe(listAddress: string, subscriberAddress: string): Promise<void>
unsubscribe(listAddress: string, subscriberAddress: string): Promise<void>
```

> **Status: not yet implemented.** `createList`, `listMembers`, `subscribe`, and `unsubscribe` currently throw `Distribution list management is not implemented` in the shipped transports; the M:N distribution-list model is planned, not current behavior.

`ListInfo` includes: address, name, member count, creation date. When an agent joins a conversation via `conversation.join`, the harness subscribes the agent to the corresponding list. When it leaves via `conversation.leave`, the harness unsubscribes.

## Transport Interface

The transport interface abstracts SMTP and IMAP behind a TypeScript API. Implementations provide the actual protocol handling; the harness uses the interface without knowing whether messages are traveling over the network or through memory.

The interface splits into three concerns: outbound delivery, inbox management, and real-time notification.

### Failures

A transport rejects with `MessageTransportError` when the condition it failed under is one a caller has to tell apart from the others. The condition is a code from RFC 5530 § 3: `NONEXISTENT` for a mailbox that is not there, `CANNOT` for an operation the transport refuses outright and that reissuing the same call does not address, `SERVERBUG` for a transport that violated one of its own invariants. Branch on the condition with `isMessageTransportError`, never on the text of the message — each transport words its message differently.

Every tool path that names a mailbox maps `NONEXISTENT` to `invalid_mailbox`, whether the name arrived as a `mailbox` argument, inside a `ref`, or as the `INBOX` that `mail.expunge` sweeps without being told to. The condition is raised before the operation, so such a result means nothing was read and nothing was written. `CANNOT` maps to `not_available` on every one of those paths, because an operation the transport refused outright is not addressed by reissuing the same call and so must not arrive under a code that invites one. `SERVERBUG`, and a rejection naming no condition, carry the code the tool answers for its own operation instead — `search_failed` for `mail.search`, for any search of `mail.wait`, and for the watch `mail.wait` installs, `not_found` for `mail.reply` and for a `mail.read` of the headers, `fetch_failed` for a `mail.read` of the whole message and for a `mail.wait` read-back, `invalid_part` for a part path, `flag_failed` for `mail.flag`, `expunge_failed` for `mail.expunge`. Those codes leave the outcome unknown. A rejection that names no condition is reported the same way, except in `mail.read`, and except in the watch `mail.wait` installs, where it is `internal_error`.

A sidecar whose inbound surface is unwired raises `SERVERBUG` from every inbound method. The fault therefore reaches the caller under whichever code the tool answers for its own operation. The unwired state is permanent, so no retry clears it.

`mail.send` and `mail.reply` take the same mapping through their own send path: a `CANNOT` there arrives as `not_available`, and a rejection naming no condition or naming `SERVERBUG` arrives as `send_failed`. Neither `send` implementation in this tree raises `NONEXISTENT` -- both reach a mailbox-name guard nowhere -- so `invalid_mailbox` is not a code a send answers today.

A rejection naming no condition tells `mail.read` nothing about whether the message is there, so it re-reads the headers before it answers: a reference the headers still answer for names a message that exists, and the failure is reported as `fetch_failed` for a whole-message read or `invalid_part` for a part path. A reference the headers do not answer for is `not_found`, whichever of the four `parts` modes the call named.

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

`fetchPart` retrieves a single MIME part by dot-separated path (IMAP `BODY.PEEK[path]`). Used to fetch just the text or JSON payload (`1.1`) or just an attachment (`1.2+`) without downloading the entire message. A part under a transfer encoding the decoder does not recognize reports `application/octet-stream`, per RFC 2045 section 6.4, and carries its octets undecoded.

`fetchFull` retrieves the complete message, parses the MIME structure, verifies the PGP signature, and returns a fully parsed `InboundMessage` with structured payload, headers, and attachments. The returned `InboundMessage` includes a `signatureStatus` field: `"valid"` (signature verified against sender's public key), `"invalid"` (signature check failed — tampering or wrong key), `"unknown"` (public key not available for verification), or `"missing"` (message was not signed).

A body the decoder cannot turn into text -- an unrecognized transfer encoding, or encoded data that will not decode -- is carried as an absent `content` (or `payload`) rather than refusing the message: the rest of the `InboundMessage` is intact, and the octets stay reachable through `fetchPart`. A structured payload that decoded but is not valid JSON is a different condition and is reported as a failure.

> **Planned / Not Yet Implemented.** Nothing branches on the `signatureStatus` carried on a delivered message. The field is computed at fetch time and passed to the agent — `mail.read` with `parts: "full"` returns it — but no harness reads it to decide anything.
>
> This is a statement about the delivered field only. A badly-signed message does not reach an agent: the recipient's sidecar verifies the signature at its delivery boundary and drops anything the admission policy does not admit, and an unsigned or unverifiable message is rejected there by default. See `decideInboundAdmission` in `packages/hub-agent/src/ws/inbound-signature.ts`, and [`INBOUND_MAIL_POLICY.md`](./INBOUND_MAIL_POLICY.md) for the `inboundMailPolicy` an author widens it with.

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

**IMAP IDLE constraint:** Standard IMAP IDLE monitors only the currently selected mailbox. Watching multiple mailboxes requires multiple IMAP connections. The interface permits multiple concurrent `watch()` calls — the implementation is responsible for the underlying connection management. No transport that speaks SMTP or IMAP over a network ships today, so there is no connection pool to describe; `@intx/mail-memory` serves every `watch()` call from its in-process store and holds no connections at all.

### Synchronization

```
sync(mailbox: string, knownState: SyncState): Promise<SyncResult>
```

Efficient reconnection using QRESYNC semantics. The harness provides its last known state (UIDVALIDITY, UIDNEXT, HIGHESTMODSEQ, known UIDs). The transport returns:

- `vanished`: UIDs that were expunged since last sync
- `changed`: messages whose flags changed since last sync
- `new`: messages that arrived since last sync

If UIDVALIDITY has changed (mailbox was recreated), the transport signals a full resync is required.

## Mail Tools

The agent interacts with the message transport through tools provided by the `@intx/tools-mail` package, composed into the runtime by the sidecar alongside other tool packages. These tools are what the inference layer presents to the model. They map to the transport interface operations.

Every tool below returns `{ error: string, code: string }` on failure. The `MailToolErrorCode` union in `@intx/tools-mail` is the authoritative list of codes; the per-tool lists below name the subset each tool can produce. Two codes belong to the runner rather than to any one tool and are absent from those lists: `unknown_tool` for a call naming a tool the runner does not provide, and `internal_error` for a handler that throws instead of returning a result. `mail.wait` names `internal_error` in its own list because it also returns that code directly.

A call whose arguments do not match the tool's declared shape, or whose arguments contradict each other, is rejected with `invalid_arguments` before the tool does any work.

`attachments`, described below for `mail.send` and `mail.reply`, is documented ahead of its implementation: neither tool declares it, so a call that carries it is refused.

The lists cover the implemented tools. `mail.threads`, `mail.move`, and the offering tools are marked below as not yet implemented; the codes in those sections describe an intended design rather than anything the tree emits.

### Tool Definitions

**mail.send** — Send mail to one or more recipients.

Parameters:

- `to`: recipient address or array of addresses
- `subject`: conversation topic (optional, carried forward in replies)
- `content`: text content of the message (for `conversation.message` type)
- `payload`: structured payload object (optional — for non-conversation types, replaces `content` with the full `body` object for the given `type`)
- `inReplyTo`: Message-ID being replied to (optional — sets In-Reply-To, and the References chain names that one parent alone)
- `correlationId`: links this message to a pending request (optional)
- `type`: Interchange payload type (default: `conversation.message`)
- `attachments`: array of `{ name, contentType, data }` (optional)

When `type` is a conversation type, the `content` string becomes the `text/plain` message body. For structured types, the `payload` object becomes the `body` field of the `application/vnd.interchange+json` part. Exactly one of the two must be present, and it must be the one the `type` takes: providing both, providing neither, or providing the field the `type` does not take is rejected as `invalid_arguments` before anything is sent.

The advertised JSON Schema does not state that pairing; the tool description carries it in prose and the handler enforces it.

Returns on success: `{ messageId: string }`.

Returns on error: `{ error: string, code: string }`. Error codes: `invalid_arguments` (the call is malformed -- an undeclared argument key, an unknown `type`, a body that contradicts the `type`, or a value no header field body can carry), `not_available` (the transport refused the send outright), `send_failed` (the transport rejected the submission for a reason of its own, an unresolvable recipient included). A `send_failed` leaves the outcome unknown rather than meaning nothing was sent; a `not_available` means the send was refused rather than attempted, and reissuing it unchanged does not make it available. No size limit is enforced on the send path, so there is no `too_large`; an oversized message reaches the transport and fails there as `send_failed` if the transport refuses it.

The send opens no correlation of its own, whatever `correlationId` it carries. To await a correlated response, call `mail.wait` after the send.

The `correlationId`, the `subject`, the `inReplyTo` and each address in `to` become a header field body, which RFC 5322 § 2.2 allows no CR and no LF, so a value carrying either is rejected as `invalid_arguments` before the send. RFC 5322 § 3.6.4 gives `In-Reply-To` as `1*msg-id` and § 3.4 gives the `To` field body as an address list, so a blank `inReplyTo` and a blank recipient each name nothing and are rejected there too, as is an empty `to` array, which names no destination at all. Whether a recipient address resolves is the transport's to answer, not the tool's: an unresolvable one is a `send_failed`. The tool constrains nothing else about the correlation id: the correlation matches on the whole string.

**mail.reply** — Reply to a specific message. Convenience wrapper around `mail.send` that automatically sets `inReplyTo` and extends the `References` chain from the parent message.

Parameters:

- `ref`: reference to the message being replied to (from search or read results)
- `content`: text content (for conversation replies)
- `payload`: structured payload object (optional, for non-conversation reply types)
- `type`: Interchange payload type (default: `conversation.message` — use `offering.response` when replying to an offering request)
- `attachments`: optional

The reply body obeys the same rule as `mail.send`: exactly one of `content` and `payload`, and it must be the one the `type` takes.

Returns on success: same as `mail.send`.

Returns on error: `{ error: string, code: string }`. Error codes: `invalid_arguments` (as for `mail.send`, including a body that contradicts the `type`), `invalid_mailbox` (the mailbox named in `ref` does not exist), `not_found` (the message being replied to could not be fetched), `not_available` (the transport refused the fetch of the parent outright), `no_reply_address` (the parent carries no From header, so it names nobody to reply to -- send to an explicit recipient with `mail.send` instead), `send_failed` (as for `mail.send` -- the reply goes through the same send path, so an error whose condition is unnamed leaves the outcome unknown there too; a refused send arrives as `not_available`).

A reply carries the parent's `Interchange-Correlation-ID` forward when the parent has one. The value is read from the parent rather than taken as an argument, so the reply cannot stamp a correlation the parent does not carry. Which correlation the reply answers is the responder's choice, made by choosing the parent: any correlated message in the mailbox is a usable `ref`. See Correlation Security above for what that means — a correlation resolves on the header alone.

**mail.search** — Search the inbox.

Parameters:

- `mailbox`: mailbox to search (default: `INBOX`)
- `query`: search criteria (structured object matching the SearchQuery type)
- `limit`: maximum results, a positive whole number (default: 20)

Date filters are given as date strings, which the tool parses into the Date instances `SearchQuery` declares. A date string it cannot parse is rejected as `invalid_query`.

The filters `SearchQuery` declares are the only ones the query accepts, at the top level and inside every `and`, `or`, and `not` branch. A key the shape does not declare is rejected as `invalid_query`, with the key named. The three parameters above are likewise the only arguments the tool accepts; an undeclared argument is rejected with the key named rather than falling back to a default.

`limit` is a positive whole number of results. Zero, a negative number and a fraction are each rejected as `invalid_arguments`. There is no value meaning "all" and no upper bound: a `limit` larger than the mailbox holds returns all of it.

Returns on success: `{ results, matched, truncated }`. Each entry of `results` carries the message `ref` and the summary fields projected from its headers — `from`, `subject`, `date`, `interchangeType` and `messageId` — and no body. An entry whose headers could not be read carries `headersError` with the reason in place of those fields, so a corrupt index is not read as a message that carries no headers. `matched` is the number of messages the query matched before `limit` was applied, and `truncated` says whether `limit` cut the list short, which is what tells a mailbox holding exactly `limit` matches from one holding hundreds.

Returns on error: `{ error: string, code: string }`. Error codes: `invalid_arguments`, `invalid_mailbox` (mailbox does not exist), `invalid_query` (malformed search criteria), `not_available` (the transport refused the search outright), `search_failed` (the transport rejected the search for a reason of its own, leaving the outcome unknown). The query is never the cause of a `search_failed` -- it was validated before the call -- so the same call is worth retrying, though a retry does not always make progress: a `search_failed` whose cause is a wiring fault stands until the wiring is fixed. An `invalid_mailbox` needs a different `mailbox`, and a `not_available` is answered by neither: the search was refused rather than attempted, and reissuing it unchanged does not make it available.

**mail.read** — Read a specific message.

Parameters:

- `ref`: message reference (from search results)
- `parts`: which parts to fetch — `"headers"`, `"payload"`, `"full"`, or a specific MIME part path like `"1.3"` (default: `"payload"`)

Returns: the requested content. For `"headers"`, returns the whole parsed header set, with no projection over its fields. For `"payload"`, returns the parsed `application/vnd.interchange+json` object. For `"full"`, returns the complete parsed message including signature status.

Returns on error: `{ error: string, code: string }`. Error codes: `invalid_arguments`, `invalid_mailbox` (the mailbox named in `ref` does not exist), `not_found` (the reference names no message), `not_available` (the transport refused the read outright), `fetch_failed` (the message is there but could not be read back -- a structured payload that is not valid JSON, say), `invalid_part` (the message is there but the requested MIME part could not be fetched).

**mail.threads** — Get conversation threads.

> **Planned / Not Yet Implemented.** This tool is not exposed by `@intx/tools-mail` today. The implemented tool set is `mail_send`, `mail_reply`, `mail_search`, `mail_read`, `mail_wait`, `mail_flag`, and `mail_expunge`. The specification below records the intended design; an agent cannot call `mail.threads` yet.

Parameters:

- `mailbox`: mailbox to thread (default: `INBOX`)
- `query`: optional search criteria to filter which messages are threaded
- `limit`: maximum threads (default: 10)

Returns: array of thread trees, each with message summaries and child threads.

**mail.flag** (`mail_flag`) — Set or clear flags on a message.

Parameters:

- `ref`: message reference
- `set`: flags to add (system flags like `\Deleted`, or custom keywords)
- `clear`: flags to remove

Provide exactly one of `set` or `clear` — one direction per call. A single call can change several flags in that direction (e.g. `set: ["\Seen", "\Flagged"]`), but adding and removing in one call is rejected.

Returns: `{ ok: true }`

Returns on error: `{ error: string, code: string }`. Error codes: `invalid_arguments` (neither `set` nor `clear`, or both), `invalid_mailbox` (the mailbox named in `ref` does not exist), `not_available` (the transport refused the mutation outright), `flag_failed` (the transport rejected the flag mutation). A `flag_failed` leaves the outcome unknown rather than meaning the mailbox is unchanged; re-read the message to learn whether the flag stuck. An `invalid_mailbox` leaves the mailbox unchanged, and `ref` is what has to change. A `not_available` leaves the mailbox unchanged too, and `ref` is not what has to change: the mutation was refused before it was attempted, and no retry of the same call makes it available.

**mail.expunge** (`mail_expunge`) — Permanently remove every `\Deleted` message from the INBOX.

Takes no parameters. Flag a message `\Deleted` with `mail.flag` first, then call this to consume it.

Returns: `{ ok: true, expungedUids: number[] }` — the uids removed.

Returns on error: `{ error: string, code: string }`. Error codes: `invalid_arguments`, `invalid_mailbox` (the INBOX does not exist), `not_available` (the transport refused the sweep outright), `expunge_failed` (every other transport rejection). An `expunge_failed` leaves the outcome unknown rather than meaning nothing was removed; an `invalid_mailbox` and a `not_available` each mean the sweep was refused before it began, so nothing was removed. The call names no mailbox, so none of the three is fixed by changing an argument.

The expunged message bytes are removed from the live mailbox but retained in the workflow run's git history, so the audit trail is preserved.

**mail.move** — Move a message to a different mailbox.

> **Planned / Not Yet Implemented.** This tool is not exposed by `@intx/tools-mail` today. The implemented tool set is `mail_send`, `mail_reply`, `mail_search`, `mail_read`, `mail_wait`, `mail_flag`, and `mail_expunge`. The specification below records the intended design; an agent cannot call `mail.move` yet.

Parameters:

- `ref`: message reference
- `to`: destination mailbox name

Returns: `{ ok: true }`

Returns on error: `{ error: string, code: string }`. Error codes: `not_found`, `invalid_mailbox`.

**mail.wait** — Block until a message matching a query arrives.

Parameters:

- `query`: search criteria (same shape as `mail.search` query — e.g. `{ from: "agent@..." }`)
- `timeout`: maximum seconds to wait (default: 120)
- `mailbox`: mailbox to watch (default: `INBOX`)

The query is parsed exactly as `mail.search` parses it, and the three parameters above are the only arguments the tool accepts.

`timeout` is a whole number of seconds from 1 to 1740; a value outside that range is rejected as `invalid_arguments`. The ceiling is 29 minutes, the longest span RFC 2177 § 3 contemplates for one IMAP IDLE. To wait longer, call the tool again.

Checks for existing matches first via `search`. If none found, subscribes to the transport's `watch` mechanism and blocks until a matching `exists` event fires or the timeout expires. The deadline and the abort signal cover the first `search` as well as the wait that follows it.

Returns on success: `{ ref, from, subject, content }` — the matched message's reference, sender, subject, and text content.

Returns on error: `{ error: string, code: string }`. Error codes: `invalid_arguments`, `invalid_mailbox` (mailbox does not exist), `invalid_query` (malformed search criteria), `not_available` (the transport refused a search, a read-back, or the watch install outright), `search_failed` (the transport rejected a search, whether the opening one or one an arrival triggered, or rejected the watch install, as in `mail.search`), `timeout` (no matching message arrived within the deadline), `aborted` (reactor shut down while waiting), `fetch_failed` (a message the tool observed to exist could not be read back; a mailbox removed while the wait holds it fires no event, so the removal goes unnoticed and the call ends in `timeout`), `internal_error` (a watch install that failed without naming a condition, or a defect in the tool package).

Use this instead of polling `mail.search` in a loop. The blocking behavior is transparent to the reactor — the tool's promise simply takes longer to resolve, and the agent naturally idles until it does.

### Offering Tools

Offering tools are convenience wrappers over the message transport for the common pattern of invoking another agent's offering and receiving the result. They construct the correct payload types and handle correlation.

**offering.invoke** — Invoke an offering on another agent.

> **Planned / Not Yet Implemented.** This tool is not exposed by `@intx/tools-mail` today. The implemented tool set is `mail_send`, `mail_reply`, `mail_search`, `mail_read`, `mail_wait`, `mail_flag`, and `mail_expunge`. The specification below records the intended design; an agent cannot call `offering.invoke` yet.

Parameters:

- `to`: target run address
- `offeringId`: the offering to invoke
- `parameters`: offering-specific parameters (object)

Internally sends an `offering.request` message with the correct payload structure and `Interchange-Offering-ID` header. Always returns a pending marker with a correlation ID, since offering invocations are inherently asynchronous.

Returns: `{ messageId: string, status: "pending", correlationId: string }`

Returns on error: `{ error: string, code: string }`. Error codes: `invalid_address`, `send_failed`.

**offering.discover** — Query another agent's available offerings.

> **Planned / Not Yet Implemented.** This tool is not exposed by `@intx/tools-mail` today. The implemented tool set is `mail_send`, `mail_reply`, `mail_search`, `mail_read`, `mail_wait`, `mail_flag`, and `mail_expunge`. The specification below records the intended design; an agent cannot call `offering.discover` yet.

Parameters:

- `to`: target run address

Sends an `offering.discover` message. Returns a pending marker; the catalog arrives as a correlated `offering.catalog` response.

Returns: `{ messageId: string, status: "pending", correlationId: string }`

### Pending Marker Pattern

A tool opens a correlation by returning a pending marker. The marker is `pendingMarker` on `ToolResult` — a sibling of `content`, not a field inside it. The harness reads it and the model never sees it. Marker fields written into `content` are text to the model and register nothing.

> **Planned / Not Yet Implemented.** No shipped tool returns a `pendingMarker`. `mail.send` returns `{ messageId }` and nothing else, so sending an `offering.request` through it opens no correlation, and the offering tools above do not exist. The reactor side of the handshake is real — `examples/agent-rich-tool` drives it end to end — but no mail tool reaches it. To await a correlated response today, call `mail.wait` after `mail.send`.

The reactor registers the correlation ID in its async state. The marker declares two fields, `status` and `correlationId`; the correlation ID is its only matching criterion, and the marker names no expected responder. A sender requirement belongs to the validator instead (see Correlation Security). The plugin sees the pending marker and decides the wait strategy: suspend at a gate, continue working, or fork a child to wait. See INFERENCE.md (Tool Execution Semantics) for the full pattern.

When the response arrives, the reactor matches it against the registered correlation, and a configured validator checks the sender and signature conditions (see Correlation Security for which of the three conditions are enforced today). On success, the reactor clears the gate, injects the resolution, and emits a `message.correlated` event.

## In-Memory Transport

For development, testing, and the initial prototype, an in-memory transport implements the full interface without network I/O. Messages are routed through memory within a single process.

The in-memory transport maintains:

- A map of run addresses to mailbox stores (each store is a map of mailbox name to message array)
- A UID counter per mailbox
- A MODSEQ counter per mailbox
- A set of watch callbacks per mailbox

`send()` assembles the message structure, signs it using the provided CryptoProvider, assigns a Message-ID, and delivers it directly to the recipient's INBOX by appending to the array and incrementing the UID counter. Watch callbacks are then scheduled with `queueMicrotask`, so they run after `send()` returns to its caller rather than on the sender's call stack — the asynchronous delivery required under Real-Time Notification above.

Search, fetch, thread, flag, and move operations are array operations over the in-memory store. THREAD REFERENCES is implemented as the RFC 5256 algorithm over the stored messages. BODYSTRUCTURE is computed from the message's MIME metadata. Partial fetch returns the requested part from the parsed structure.

The in-memory transport is not a mock. It implements the full interface with correct semantics — UID ordering, MODSEQ tracking, flag persistence, thread construction. The only difference from a real SMTP/IMAP transport is that messages do not traverse a network.
