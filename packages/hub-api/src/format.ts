export function ts(d: Date): string {
  return d.toISOString();
}

export function first<T>(rows: T[]): T {
  const row = rows[0];
  if (!row) throw new Error("Expected at least one row from returning()");
  return row;
}
