import { ApiError } from "@/lib/api";

export function MutationError({ error }: { error: Error | null }) {
  if (!error) return null;

  const message =
    error instanceof ApiError
      ? `${error.message} (${error.code})`
      : error.message;

  return (
    <p className="text-sm text-destructive" role="alert">
      {message}
    </p>
  );
}
