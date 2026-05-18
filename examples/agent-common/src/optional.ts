// Helper that turns the conditional-spread idiom
//
//   ...(opts.foo !== undefined ? { foo: opts.foo } : {})
//
// into
//
//   ...optional("foo", opts.foo)
//
// The verbose inline form is otherwise required at every call
// because the repo's `exactOptionalPropertyTypes` setting rejects
// `{ foo: undefined }` for a `foo?: T` field. The helper exists so
// the examples can demonstrate the agent surface without dragging
// that idiom into every line that hands a value to `createAgent`.

/**
 * Returns `{ [key]: value }` when `value` is defined, or `{}`
 * otherwise. Spread the result into an options object to pass
 * `value` only when present.
 */
export function optional<K extends string, V>(
  key: K,
  value: V | undefined,
): Partial<Record<K, V>> {
  if (value === undefined) return {};
  // TypeScript widens the inferred type of `{ [key]: value }` to
  // `{ [k: string]: V }` when K is a generic string literal because
  // computed-property keys cannot be tracked back to the literal
  // type parameter. The assertion narrows the inferred dynamic-key
  // shape to the declared `Partial<Record<K, V>>` — the runtime
  // object is exactly that shape by construction.
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- generic-key computed-property limitation; runtime shape matches by construction
  return { [key]: value } as Partial<Record<K, V>>;
}
