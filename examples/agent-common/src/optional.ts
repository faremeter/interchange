// Helper that turns the conditional-spread idiom
//
//   ...(opts.foo !== undefined ? { foo: opts.foo } : {})
//
// into
//
//   ...optional("foo", opts.foo)
//
// The verbose inline form is otherwise required at every call because
// the repo's `exactOptionalPropertyTypes` setting rejects
// `{ foo: undefined }` for a `foo?: T` field.

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
  // `{ [k: string]: V }` for a generic key because computed-property
  // keys cannot be tracked back to the literal type parameter; the
  // assertion narrows it to the declared `Partial<Record<K, V>>`.
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- generic-key computed-property limitation; runtime shape matches by construction
  return { [key]: value } as Partial<Record<K, V>>;
}
