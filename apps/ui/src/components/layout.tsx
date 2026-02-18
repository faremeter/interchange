import { Link, Outlet, useRouter } from "@tanstack/react-router";
import { useQuery } from "@tanstack/react-query";

import { meProfileQuery } from "../lib/queries/me";
import { auth } from "../lib/auth";

export function Layout() {
  const { data: profile } = useQuery(meProfileQuery);
  const router = useRouter();

  const handleSignOut = async () => {
    await auth.signOut();
    await router.navigate({ to: "/login" });
  };

  return (
    <div className="min-h-screen bg-gray-50">
      <nav className="border-b border-gray-200 bg-white">
        <div className="mx-auto flex h-14 max-w-6xl items-center justify-between px-4">
          <Link to="/" className="text-lg font-semibold text-gray-900">
            Interchange
          </Link>
          <div className="flex items-center gap-4">
            {profile && (
              <span className="text-sm text-gray-600">{profile.name}</span>
            )}
            <button
              onClick={() => void handleSignOut()}
              className="text-sm text-gray-500 hover:text-gray-700"
            >
              Sign out
            </button>
          </div>
        </div>
      </nav>
      <main className="mx-auto max-w-6xl px-4 py-6">
        <Outlet />
      </main>
    </div>
  );
}
