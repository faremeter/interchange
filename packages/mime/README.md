# @intx/mime

RFC 2822 message assembly and parsing. Builds multipart/signed
messages with PGP detached signatures, parses inbound wire bytes
back into structured parts, and owns the JMAP-shaped envelope the
rest of the mail pipeline depends on.

Consumed by `@intx/mail-memory` (in-process transport),
`@intx/storage-isogit` (mail audit log), and `@intx/harness`
(sidecar mail-tool plumbing).

```ts
import {
  parseHeaderSection,
  parseMultipart,
  extractBoundary,
} from "@intx/mime";

const { headers, bodyOffset } = parseHeaderSection(rawMessageBytes);
const contentType = headers.get("content-type");
if (contentType === undefined) throw new Error("missing Content-Type");
const boundary = extractBoundary(contentType);
if (boundary === undefined) throw new Error("missing boundary parameter");
const parts = parseMultipart(rawMessageBytes.subarray(bodyOffset), boundary);
```

For outbound mail the builder layer is the entry point:
`createOutboundMessage` produces a structured envelope,
`assembleSignedContent` canonicalises the signed body, and
`assembleMessage` joins the signed content with a detached
signature from `createDetachedSignatureFromProvider` into wire
bytes.
