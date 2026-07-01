// Invertible codec for tool names on the provider wire.
//
// Internal tool names are package-qualified ids like
// `@intx/tools-posix/sidecar-bundle:run_shell`. Provider function-name
// charsets are narrow (OpenAI `^[a-zA-Z0-9_-]{1,64}$`, Anthropic 128, Gemini
// with a leading-letter rule), and reject the `@`, `/`, `:`, and `.`
// characters these ids carry. This codec maps such a name to a
// wire-charset-safe form and back.
//
// Invariant: `decodeToolName(encodeToolName(x, ...)) === x`. It is
// load-bearing. Tool-call dispatch keys on the exact prefixed name in two
// places (the agent's `byName` map and the tool-package loader's per-bundle
// `nameMap`), so a name that does not round-trip lands as an `unknown tool`
// with no error at the point of the fault. The `encode`/`decode` naming
// advertises the invertibility on purpose: a `sanitize`-style name invites a
// future lossy "cleanup" that would break dispatch.
//
// Names that are already valid on the wire pass through untouched — the codec
// only rewrites names that genuinely need it. Rewritten names carry a
// distinctive `MARKER` prefix, and `decode` transforms only marker-prefixed
// names, so an ordinary wire-valid name a provider echoes (a tool the model
// named that never needed encoding, or a hallucination) is returned verbatim.
// The marker is what makes the round-trip unambiguous: a name that is already
// valid but happens to begin with the marker is force-encoded too, so a
// marker prefix on the wire always denotes an encoding.
//
// Rewriting escapes each out-of-charset character (and, so the sentinel stays
// unambiguous, each literal `-`) as `-XX`, its uppercase two-digit hex byte:
// `@`->`-40`, `/`->`-2F`, `:`->`-3A`, `.`->`-2E`, `-`->`-2D`. A `base64url`
// encoding (reusing `@intx/types/base64url`) was considered and rejected: it
// renders every name fully opaque, hurting both model tool-selection and
// debugging, and it would encode even the already-legible names this scheme
// leaves alone.

// A distinctive, letter-leading prefix that ordinary tool names do not start
// with. Letter-leading satisfies providers (Gemini) that require a
// letter/underscore leading character on every function name.
const MARKER = "IX_";

// Characters that survive a rewrite verbatim. `-` is deliberately excluded so
// it can serve as the escape sentinel inside a rewritten name.
const ESCAPE_PASSTHROUGH = /^[A-Za-z0-9_]$/;
// The provider wire charset. A name already matching this, that starts with a
// letter or underscore and does not collide with the marker, needs no rewrite.
const WIRE_SAFE = /^[A-Za-z_][A-Za-z0-9_-]*$/;
const HEX_PAIR = /^[0-9A-Fa-f]{2}$/;

// The wire-name length ceiling for a provider, with a label used in the loud
// error a too-long name raises. The limit is provider-specific, so the
// adapter that binds the provider owns the constant and passes it in.
export type ToolNameLimit = {
  readonly provider: string;
  readonly maxLength: number;
};

function needsRewrite(name: string): boolean {
  return !WIRE_SAFE.test(name) || name.startsWith(MARKER);
}

function rewrite(name: string): string {
  let out = MARKER;
  for (const ch of name) {
    if (ESCAPE_PASSTHROUGH.test(ch)) {
      out += ch;
      continue;
    }
    const code = ch.charCodeAt(0);
    if (code > 0xff) {
      throw new Error(
        `Cannot encode tool name "${name}": character "${ch}" is outside the ` +
          `single-byte range the wire codec supports.`,
      );
    }
    out += "-" + code.toString(16).toUpperCase().padStart(2, "0");
  }
  return out;
}

// Encode a tool name into a form valid for the provider's function-name
// charset. Names already valid on the wire pass through unchanged. Throws if
// the resulting wire name exceeds the provider's length limit — on the
// passthrough path too, since an already-valid name can still be too long — so
// a name too long for a provider surfaces as a fixable diagnostic rather than
// truncation, a silent collision, or an opaque upstream 400.
export function encodeToolName(name: string, limit: ToolNameLimit): string {
  const wire = needsRewrite(name) ? rewrite(name) : name;
  if (wire.length > limit.maxLength) {
    throw new Error(
      `Tool name "${name}" is ${wire.length} chars on the wire, which ` +
        `exceeds the ${limit.maxLength}-char limit for provider ` +
        `"${limit.provider}". Shorten the tool bundle id or tool name.`,
    );
  }
  return wire;
}

// Invert `encodeToolName`. Total: a wire name without the marker prefix, or a
// marker-prefixed name whose body is not a valid escaping, is returned
// unchanged. That covers both names that never needed encoding and
// hallucinated or provider-mangled names, which then fall through to the
// existing `unknown tool` handling — giving the model feedback to retry —
// rather than throwing and tearing down the stream over a bad tool name.
export function decodeToolName(wire: string): string {
  if (!wire.startsWith(MARKER)) {
    return wire;
  }
  const body = wire.slice(MARKER.length);
  let out = "";
  let i = 0;
  while (i < body.length) {
    const ch = body.charAt(i);
    if (ch === "-") {
      const hex = body.slice(i + 1, i + 3);
      if (!HEX_PAIR.test(hex)) {
        return wire;
      }
      out += String.fromCharCode(parseInt(hex, 16));
      i += 3;
      continue;
    }
    if (!ESCAPE_PASSTHROUGH.test(ch)) {
      return wire;
    }
    out += ch;
    i += 1;
  }
  return out;
}
