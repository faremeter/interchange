# Faremeter Interchange

_Mail over a real SMTP relay and IMAP mailbox_

**Status: spike.** The production path carries mail as frames on the
hub/sidecar control socket. This document describes an alternative
backing that puts the same mail on a real SMTP submission relay and a
real IMAP mailbox, and the properties a third-party mail server imposes
on the system around it. None of it is adopted.

## Scope

The backing covers two boundaries:

- **The agent mailbox.** An agent's inbox is a mailbox on an IMAP
  server. The agent reads it through the same mail tools it uses over
  every other backing.
- **The hub-to-sidecar hop.** The hub submits a message over SMTP
  instead of pushing a frame down the control socket. The recipient
  receives it through its own mailbox.

External federation is out of scope. Nothing here signs a message for a
foreign domain or validates one that arrives from outside.

## What the transport asks of a server

The message transport declares ten methods. They need base IMAP4rev1
`SEARCH` and `FETCH`, `IDLE` for arrival notification, and support for
arbitrary keywords. They do not need `THREAD`, `CONDSTORE`, `QRESYNC`,
or `MOVE`.

That matters when a server is chosen. Each capability the interface
requires narrows the set of servers that can host an agent, and a
hosted provider is free to omit an extension nothing obliges it to
offer. Threading and body-structure projections run over the mailbox
model in the client, so the server is not asked to compute them.

The reference server is Postfix with Dovecot, from the
`mailserver/docker-mailserver` image. Stalwart does not fit: its
configuration lives in its own database and is reachable only through a
command line tool and a web interface, so a test suite cannot provision
accounts declaratively.

## The reload

**Creating a mailbox reloads the mail server.** A reload terminates
live sessions and refuses authentication for roughly a second.

This single property drives most of the design below. The hub creates a
mailbox inside the deploy it is serving, so every deployment causes a
reload, and that reload lands on every other deployment sharing the
server. A reload is therefore an ordinary event, not a fault.

Three consequences follow, and each has its own section: a connection
can disappear at any moment, a login can be refused for a reason that
clears, and a submission can be refused the same way.

## Connection model

A transport holds two IMAP connections.

`IDLE` monitors only the selected mailbox and occupies its connection
for as long as it runs. A transport that both serves commands and
watches for arrivals therefore needs a second connection. The **command**
connection runs `SEARCH` and `FETCH` with auto-idle disabled, because an
idle there notifies nobody while costing a break-and-resume round trip
on the next command. The **watch** connection enters idle almost
immediately, because every millisecond before it does is added to the
time an agent waits for mail.

The watch connection selects its mailbox when the transport starts, not
when a caller installs a watch. `watch` returns synchronously, so a
`SELECT` issued from inside it completes after the caller has moved on,
and an arrival in that window fires no event the caller can still
observe.

The IMAP client holds one connection and cannot reopen it. A transport
whose socket closes is finished. It reports the close once through a
hook, and the owner recovers by discarding the transport and building a
new one. For the same reason, a login retry constructs a fresh pair of
clients per attempt rather than reconnecting the old ones.

## Mailbox visibility across two connections

Dovecot answers the first `UID SEARCH` that follows an arrival with an
empty result, and sends the `EXISTS` untagged response afterwards. With
the watch and the command on separate connections, the watch learns
about the arrival and the command connection does not.

The command connection therefore issues a `NOOP` before its next
command whenever the watch has observed an arrival it has not yet
accounted for. The `NOOP` is what delivers the pending untagged
responses to that connection.

A failed `SEARCH` needs care for a second reason: the client reports it
by **returning** `false` rather than by rejecting. A returned `false` is
narrowed and raised as a server fault, because treating it as an empty
result would report "no matching messages" for a search that never ran.

## Failure classification

Three paths talk to the mail server: SMTP submission from the hub's
relay, SMTP submission from an agent's own `send`, and IMAP login when a
transport starts. All three retry a refusal that clears on its own, with
bounded attempts and a doubling delay.

What counts as transient is the substantive decision, and it departs
from both protocols in the same place:

| Condition                               | Retried | Why                                                           |
| --------------------------------------- | ------- | ------------------------------------------------------------- |
| SMTP 4xx                                | yes     | RFC 5321 defines it as transient                              |
| Connection or socket failure            | yes     | the server never saw the message                              |
| SMTP `535`, IMAP `AUTHENTICATIONFAILED` | yes     | see below                                                     |
| IMAP `UNAVAILABLE`, `INUSE`             | yes     | the server says it cannot serve this session yet              |
| SMTP 5xx on an envelope or message      | no      | an unknown recipient or an oversized message does not improve |
| A refusal naming no condition           | no      | a defect in our own code, which must stay visible             |

Authentication failure is the departure. Both protocols class it as
permanent, and the honest reading of a `535` or a tagged `NO` is that
the credential is wrong. That reading is wrong here: a server mid-reload
answers exactly this to an account whose password is correct and which
works a moment later.

The asymmetry decides it. Retrying a credential that really is wrong
costs a few attempts and then reports the same refusal. Not retrying
costs a deployment its run, because a refused submission inside a step
fails the step, a failed step fails the run, and a failed run is
terminal.

A reported failure says which of the two outcomes occurred: a permanent
refusal, or a transient one that outlasted the attempts. A caller
answers differently for each.

## Mailbox lifecycle

The hub owns both ends of a mailbox's life, because it owns the address.

**Creation** happens inside the deploy, at one ingress. The deploy frame
then carries the credential to the one recipient entitled to it. A
failure to provision fails the deploy: a deployment with no mailbox
cannot receive its trigger, so a frame sent anyway would produce a
silently deaf deployment.

Provisioning converges rather than merely creating. An account that
already exists has its password set to the derived value. Creation alone
would leave an account from an earlier root secret holding a password
nobody derives, which is unopenable and silently so.

**Removal** happens after the undeploy is acknowledged, and only then.
Until the sidecar acknowledges, it still holds IMAP sessions on that
mailbox. Removal is skipped entirely when an undeploy times out or
fails, because such an undeploy may have left the deployment running,
and taking a mailbox from a deployment still reading it is worse than
leaving one behind.

Removal never fails an undeploy. The undeploy has already torn the
deployment down, so a report of failure would describe the wrong thing.
A mailbox that cannot be removed is reported and left.

Any mail still in the mailbox goes with it. A deployment has one run and
is never redeployed to the same address, so the only reader that mail
ever had is gone.

## Mailbox credentials

A mailbox password is derived, not stored centrally and not shared. The
hub computes it with HMAC-SHA256 over the address, keyed by a root
secret that never leaves the hub. Each recipient receives only its own
password.

The sidecar seals each address's credential with the credential cipher,
bound by additional authenticated data naming the address and the field.
It writes the durable copy before the in-memory one, so a credential a
process can use is a credential a restarted process can still open.

A transport is built only for an address whose credential is held. A
missing credential raises rather than falling back, because a fallback
would silently reintroduce a shared account.

## Admission and authorization ordering

Mail and the authorization to act on it arrive by different routes, so
the recipient orders them explicitly.

A run's grants reach the sidecar in one of two ways. The hub pushes them
when it starts the run, and the sidecar asks for them when it needs them
and does not have them. Mail admission waits on whichever arrives first.
The wait is necessary because the admission decision needs the sender
key, which travels with the grants.

Readiness is read from durable state, keyed by the deployment's own
identity. No process-local memo records it. A memo would answer "not
ready" for a restored deployment whose grants are already committed, and
wedge a message the deployment was entitled to receive.

The proactive push is therefore a latency optimization and not a
correctness requirement. A run whose push is lost still completes,
because the sidecar asks.

## Message assembly

Assembly produces bytes that go on a wire a third party reads, so two
RFC 5322 requirements bind that a private frame format did not.

**Line length.** No header line exceeds 998 characters. Folding targets
the recommended 78 and inserts folding whitespace only where the value
already has a space, which is the only point the grammar permits. A
receiver's unfolded value is then identical to the one supplied. A single
token longer than the target is emitted whole: there is no legal fold
point inside a message id or an atom, so an over-long one stays intact
rather than being corrupted.

**Non-ASCII header text.** A subject that is not ASCII is encoded as RFC
2047 encoded-words. Base64 is used for every value that needs encoding,
because quoted-printable needs a per-character escape table whose
omissions are silent and offers a receiver nothing base64 does not.
Words are split on character boundaries, since a multi-byte sequence
straddling two of them decodes to replacement characters in both.

Inbound subjects are decoded, and both base64 and quoted-printable are
accepted, because the choice belongs to whoever sent the message. A word
whose charset or bytes cannot be decoded keeps its encoded form: the
encoded text is a visible fault a reader can report, whereas replacement
characters read as the sender's own words.

Signatures are unaffected by any of this. A detached signature covers
the signed content part, not the header section, and the relay submits
assembled bytes verbatim rather than re-serializing them.

## Running the tests

The suite drives a containerized Postfix and Dovecot that the test run
neither starts nor stops. Start-up is slow enough that paying it per run
would dominate the suite, and the container holds no per-run state:
each test provisions its own addresses.

```
bin/mail-server up
make test-mail
```

With no container reachable the suite skips every case rather than
failing, so a pass proves nothing unless the container was running.

Two properties of the server shape how the tests are written. A test
that needs a reload to drop an established connection must first confirm
the connection exists, which only the server can answer — a deploy
resolving does not mean a login has finished. And a test must not assert
on whether a reload has landed, because that is a race against other
deployments' provisioning.

## Known gaps

- A removed mailbox's credential keeps authenticating until some later
  reload, because removal does not reload the server. Exposure is
  limited: the only holder is the deployment just torn down, and the
  password opens nothing else.
- Rotating the mailbox root secret has no sweep over live deployments.
  Their sealed credentials would no longer match a derived password.
- The suite does not run in continuous integration, because it needs a
  container this repository does not manage.
- The mailbox index records tombstones that nothing reads. They exist for
  a resynchronization path the transport does not use.
