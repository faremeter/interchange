// Known-answer test vectors for the Ed25519 / OpenPGP / SSHSIG path.
//
// GENERATED FILE — do not edit by hand. Regenerate with:
//   bun run packages/crypto/src/kat-vectors.gen.ts
//
// The expected outputs were first captured from the pre-port node:crypto
// implementation at commit 76f4c96e and are reproduced by the Web Crypto
// port. Ed25519 is deterministic (RFC 8032) and the DER framing and
// OpenPGP / SSHSIG assembly are unchanged across the port, so the bytes
// are identical either way. kat.test.ts asserts the package reproduces
// these vectors — the wire-compatibility guarantee.

export const SEED_HEX =
  "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";
export const PUBLIC_KEY_HEX =
  "03a107bff3ce10be1d70dd18e74bc09967e4d6309ba50d5f1ddc8664125531b8";
export const RAW_MESSAGE = "interchange ed25519 KAT message";
export const RAW_SIGNATURE_HEX =
  "d179c7b30ebd94366f7c3f20fa83f13c26edd18195b2a8af30795d4f00ef518563b8693fc3d8b199d2ac62f3bf147a945a5d966702a68e5aff55275d61c5e600";
export const CREATION_TIME = 1700000000;
export const PGP_CONTENT = "KAT PGP content\r\n";
export const PGP_ARMORED =
  "-----BEGIN PGP SIGNATURE-----\n\nwlQEABYKAAYFAmVT8QAAANFyAQDob3LIuuScFQkOeK/CDHriKRKY2f86YFwVWlieccu98gD8XG1A\nq2KqKMniXjgLC67ttI8fg8ptNA0pKEnxQ4n6CQk=\n=Qwv0\n-----END PGP SIGNATURE-----";
export const SSH_PAYLOAD =
  "tree 0123\nauthor KAT <k@k> 1700000000 +0000\n\nkat ssh\n";
export const SSH_ARMORED =
  "-----BEGIN SSH SIGNATURE-----\nU1NIU0lHAAAAAQAAADMAAAALc3NoLWVkMjU1MTkAAAAgA6EHv/POEL4dcN0Y50vAmWfk1j\nCbpQ1fHdyGZBJVMbgAAAADZ2l0AAAAAAAAAAZzaGE1MTIAAABTAAAAC3NzaC1lZDI1NTE5\nAAAAQFyN7lNTXChWzv692HGBFUCDMT2l3lIDhzTZVec+rR2cv3KGvUa6xQ9ZBK+kFt2eEm\nuUwKehUQVQTnkOzxphKA0=\n-----END SSH SIGNATURE-----";
