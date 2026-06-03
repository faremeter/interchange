# @intx/crypto-node

Node-backed cryptographic provider. Ed25519 key generation,
import, sign, and verify; canonicalisation of text and byte
payloads; SSH and PGP detached signature formats; ASCII armoring.

The exported `NodeCrypto` implements the `CryptoProvider` contract
that the mail and storage layers depend on. Consumed by
`@intx/mime` (detached PGP signatures on outbound mail),
`@intx/storage-isogit` (commit signing), `@intx/mail-memory`
(per-address keys for the in-process transport), and
`@intx/hub-sessions` (per-agent key material).

```ts
import { generateKeyPair, createNodeCrypto } from "@intx/crypto-node";

const keyPair = await generateKeyPair();
const provider = createNodeCrypto(keyPair);

const signature = await provider.sign(new TextEncoder().encode("hello"));
```
