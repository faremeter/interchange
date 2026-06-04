// Deterministic JSON serialization for deploy-hash inputs.
//
// `canonicalizeForHash` produces stable bytes from a value tree that
// participates in the deploy hash (notably `DirectorRef.config` and the
// `AgentDefinition` envelope). The output is the encoded form of a
// canonical JSON document: object keys NFC-normalized then sorted,
// strings normalized to NFC, no whitespace. Non-JSON values (Date, Map,
// Set, function, undefined, symbol, NaN, +/-Infinity) are rejected so
// the hash cannot silently absorb a value the JSON receiver could not
// reproduce.
//
// The implementation builds a normalized JS value tree first, then
// `JSON.stringify`s it. The intermediate tree lets the cycle check and
// type rejections share a single recursive walk.
//
// Key-ordering caveat. The walk sorts NFC-normalized keys
// lexicographically before assigning them into the intermediate plain
// object. `JSON.stringify` then walks the object's own keys in the
// engine's iteration order, which per ECMA-262
// (OrdinaryOwnPropertyKeys) lists integer-indexed string keys first in
// ascending numeric order, then the remaining keys in insertion order.
// For purely string-keyed maps the emitted bytes follow the algorithm's
// lex sort; for integer-keyed maps (string keys like "1", "2", "10")
// the engine re-orders the integer prefix numerically, so the emitted
// bytes do not match a strict lex sort of the same keys
// ("1","10","2"). The behavior is deterministic across every JS engine
// that implements OrdinaryOwnPropertyKeys (i.e. every engine since
// ES2020), so deploy-hash equality across producers is preserved. A
// future engine that changed this rule would change the canonical
// bytes; if that becomes a concern, replace `JSON.stringify` with a
// hand-rolled emitter that walks the sorted-key list directly.

type JsonLike =
  | null
  | boolean
  | number
  | string
  | readonly JsonLike[]
  | { readonly [key: string]: JsonLike };

const encoder = new TextEncoder();

export class CanonicalizationError extends Error {
  readonly path: readonly string[];

  constructor(message: string, path: readonly string[]) {
    super(
      path.length === 0
        ? message
        : `${message} (at ${path.length === 1 ? path[0] : path.join(".")})`,
    );
    this.name = "CanonicalizationError";
    this.path = path;
  }
}

function normalize(
  value: unknown,
  path: readonly string[],
  seen: WeakSet<object>,
): JsonLike {
  if (value === null) return null;

  if (typeof value === "boolean") return value;

  if (typeof value === "string") {
    return value.normalize("NFC");
  }

  if (typeof value === "number") {
    if (!Number.isFinite(value)) {
      throw new CanonicalizationError(
        `non-finite number (${String(value)}) is not valid JSON`,
        path,
      );
    }
    return value;
  }

  if (typeof value === "undefined") {
    throw new CanonicalizationError("undefined is not valid JSON", path);
  }

  if (typeof value === "symbol") {
    throw new CanonicalizationError("symbol is not valid JSON", path);
  }

  if (typeof value === "function") {
    throw new CanonicalizationError("function is not valid JSON", path);
  }

  if (typeof value === "bigint") {
    throw new CanonicalizationError("bigint is not valid JSON", path);
  }

  // Objects: arrays, plain records, or rejected built-ins. After the
  // primitive checks above, the only remaining narrowed type is
  // `object`.
  const obj: object = value;

  if (seen.has(obj)) {
    throw new CanonicalizationError("cycle detected", path);
  }
  seen.add(obj);

  try {
    if (Array.isArray(obj)) {
      const out: JsonLike[] = [];
      for (let i = 0; i < obj.length; i++) {
        out.push(normalize(obj[i], [...path, `[${String(i)}]`], seen));
      }
      return out;
    }

    if (
      obj instanceof Date ||
      obj instanceof Map ||
      obj instanceof Set ||
      obj instanceof RegExp ||
      obj instanceof Promise ||
      obj instanceof Error ||
      obj instanceof ArrayBuffer ||
      ArrayBuffer.isView(obj)
    ) {
      throw new CanonicalizationError(
        `${obj.constructor.name} is not valid JSON`,
        path,
      );
    }

    const proto: unknown = Object.getPrototypeOf(obj);
    if (proto !== Object.prototype && proto !== null) {
      let protoName = "unknown";
      if (
        typeof proto === "object" &&
        proto !== null &&
        "constructor" in proto &&
        typeof proto.constructor === "function"
      ) {
        protoName = proto.constructor.name;
      }
      throw new CanonicalizationError(
        `non-plain object (prototype ${protoName}) is not valid JSON`,
        path,
      );
    }

    // After the proto check, `obj` is a plain Record<string, unknown>.
    // Index it through a generic record type to drop symbol keys (which
    // Object.keys also drops).
    const record: Record<string, unknown> = Object.fromEntries(
      Object.entries(obj),
    );
    // Normalize keys to NFC before sorting and before indexing the
    // output. Two failure modes ride on this ordering: (a) if two
    // distinct raw keys normalize to the same NFC form, silently
    // overwriting one with the other would drop data and the deploy
    // hash would no longer be a faithful function of the input; (b)
    // sorting raw keys and then normalizing produces an output key
    // order that is not the canonical NFC-sorted order, so two
    // producers (one pre-normalizing, one not) would hash the same
    // logical value to different bytes. Normalize first, raise on any
    // NFC collision, then sort.
    const byNFC = new Map<string, string>();
    for (const rawKey of Object.keys(record)) {
      const nfcKey = rawKey.normalize("NFC");
      const existing = byNFC.get(nfcKey);
      if (existing !== undefined && existing !== rawKey) {
        throw new CanonicalizationError(
          `keys ${JSON.stringify(existing)} and ${JSON.stringify(rawKey)} ` +
            `NFC-normalize to the same value (${JSON.stringify(nfcKey)})`,
          path,
        );
      }
      byNFC.set(nfcKey, rawKey);
    }
    const nfcKeys = [...byNFC.keys()].sort((a, b) =>
      a < b ? -1 : a > b ? 1 : 0,
    );
    const out: Record<string, JsonLike> = {};
    for (const nfcKey of nfcKeys) {
      const rawKey = byNFC.get(nfcKey);
      if (rawKey === undefined) continue;
      out[nfcKey] = normalize(record[rawKey], [...path, rawKey], seen);
    }
    return out;
  } finally {
    seen.delete(obj);
  }
}

/**
 * Produce stable bytes for a value tree. The output is the UTF-8
 * encoded form of a canonical JSON document with sorted object keys,
 * NFC-normalized strings, and no whitespace. Throws
 * `CanonicalizationError` on any non-JSON value or cycle.
 *
 * Equality of two outputs implies equality of the canonical structural
 * form of the inputs; consumers may safely hash the output to compare
 * value identity across local-dev and production bundles.
 */
export function canonicalizeForHash(value: unknown): Uint8Array {
  const normalized = normalize(value, [], new WeakSet());
  // JSON.stringify with no replacer and no space arg produces the
  // canonical form modulo key ordering, which `normalize` has already
  // resolved by constructing plain records with sorted keys.
  return encoder.encode(JSON.stringify(normalized));
}
