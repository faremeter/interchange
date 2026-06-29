# @intx/crypto

Web Crypto-backed cryptographic provider. Ed25519 key generation,
import, sign, and verify; canonicalisation of text and byte
payloads; SSH and PGP detached signature formats; ASCII armoring.

The exported `Ed25519Crypto` implements the `CryptoProvider` contract
that the mail and storage layers depend on. Consumed by
`@intx/mime` (detached PGP signatures on outbound mail),
`@intx/storage-isogit` (commit signing), `@intx/mail-memory`
(per-address keys for the in-process transport), and
`@intx/hub-sessions` (per-agent key material).

```ts
import { generateKeyPair, createEd25519Crypto } from "@intx/crypto";

const keyPair = await generateKeyPair();
const provider = createEd25519Crypto(keyPair);

const signature = await provider.sign(new TextEncoder().encode("hello"));
```
