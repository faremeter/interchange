// Base64 codec for byte strings.
//
// Used to ship binary mail bodies over text-only WebSocket frames.
// Centralizing here keeps the encoding stable across the sidecar/hub
// boundary and any other consumer that needs the same wire shape.

export function base64Encode(bytes: Uint8Array): string {
  let binary = "";
  for (const byte of bytes) {
    binary += String.fromCharCode(byte);
  }
  return btoa(binary);
}

export function base64Decode(base64: string): Uint8Array {
  const binary = atob(base64);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) {
    bytes[i] = binary.charCodeAt(i);
  }
  return bytes;
}
