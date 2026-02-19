import { Link, Outlet, useRouter } from "@tanstack/react-router";
import { useQuery } from "@tanstack/react-query";

import { meProfileQuery } from "@/lib/queries/me";
import { auth } from "@/lib/auth";
import { Button } from "@/components/ui/button";

export function Layout() {
  const { data: profile } = useQuery(meProfileQuery);
  const router = useRouter();

  const handleSignOut = async () => {
    await auth.signOut();
    await router.navigate({ to: "/login" });
  };

  return (
    <div className="min-h-screen bg-background">
      <nav className="border-b bg-card">
        <div className="mx-auto flex h-14 max-w-6xl items-center justify-between px-4">
          <Link to="/" className="text-lg font-semibold">
            Interchange
          </Link>
          <div className="flex items-center gap-4">
            {profile && (
              <span className="text-sm text-muted-foreground">
                {profile.name}
              </span>
            )}
            <Button
              variant="ghost"
              size="sm"
              onClick={() => void handleSignOut()}
            >
              Sign out
            </Button>
          </div>
        </div>
      </nav>
      <main className="mx-auto max-w-6xl px-4 py-6">
        <Outlet />
      </main>
    </div>
  );
}
