# Faremeter Interchange

*Implementation*

## Message Bus: Email Infrastructure

The Interchange message bus is built on SMTP and IMAP, leveraging the existing global email infrastructure as the transport layer for agent communication.

### Why Email

Email provides a battle-tested, globally distributed message passing system with decades of operational maturity:

- **Universal addressing** - Every agent gets an email address as its network identity
- **Federated by default** - No central authority required; agents on different domains communicate seamlessly
- **Delivery guarantees** - SMTP provides store-and-forward semantics with retry logic
- **Asynchronous** - Agents don't need to be online simultaneously to exchange messages
- **Existing infrastructure** - DNS MX records, TLS, spam filtering, and authentication (SPF, DKIM, DMARC) already exist
- **Firewall-friendly** - Email traverses network boundaries that block other protocols

### Agent Addressing

Each agent has an email address that serves as its network identifier:

```
agent-name@domain.interchange.network
```

The local part identifies the agent; the domain identifies the Interchange deployment or organization hosting it.

### Message Transport

**Outbound (SMTP)**
When an agent sends a message to another agent or human, the kernel composes an email and submits it via SMTP. The message payload is serialized into the email body. Structured data uses MIME multipart encoding.

**Inbound (IMAP)**
The kernel maintains an IMAP connection to the agent's mailbox. Incoming messages are fetched, parsed, and converted into events that the kernel routes to internal handlers.

### Message Topologies

Email naturally supports the message bus topologies defined in the architecture:

- **1:1** - Direct email between two addresses
- **1:N** - Mailing lists or CC/BCC for broadcast
- **M:N** - Mailing lists where multiple agents can post and receive

### Authentication and Trust

Interchange uses standard email authentication mechanisms:

- **SPF** - Sender Policy Framework validates sending servers
- **DKIM** - DomainKeys Identified Mail provides cryptographic signatures
- **DMARC** - Domain-based Message Authentication ties SPF and DKIM together

Agent identity is bound to its email address and verified through these mechanisms. Additional payload-level signatures can provide end-to-end verification when required.

### Encryption

- **Transport encryption** - TLS for SMTP and IMAP connections
- **Payload encryption** - S/MIME or PGP for end-to-end encrypted messages between agents

### Spam and Abuse Prevention

Existing email infrastructure handles abuse:

- Rate limiting at the SMTP level
- Reputation systems for sending domains
- Content filtering where appropriate

Agents that misbehave lose email deliverability, which maps directly to losing network access.

### Mailbox as State

The agent's IMAP mailbox provides a natural persistence layer:

- Unprocessed messages remain in the inbox
- Processed messages can be archived or deleted
- Folders organize different message types or conversations
- Search capabilities (IMAP SEARCH) allow retrieval of historical context

### Limitations and Mitigations

**Latency**
Email is not designed for real-time communication. For latency-sensitive workloads, agents can establish direct connections after initial discovery via email.

**Message size**
Large payloads may hit email size limits. Agents can use email for signaling and exchange large data through separate channels (e.g., signed URLs).

**Ordering**
Email does not guarantee message ordering. Agents that require ordering must implement sequencing at the application layer.
