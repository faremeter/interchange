# @intx/hub-common

Utilities shared across the hub packages. This package has a generic name and
is therefore at risk of becoming a junk drawer. A new module belongs here only
if it satisfies all of the following:

1. It is genuinely needed by more than one of the hub packages
   (`@intx/hub-api`, `@intx/hub-sessions`, or future hub
   packages). Code used by exactly one package belongs in that package.
2. It encodes hub-schema-owner knowledge that does not belong in
   `@intx/types`. For example, the id prefix table here is the
   identity namespace of the hub's database schema, so it lives with the
   schema owner. By contrast, a generic hex encoder belongs in
   `@intx/types` (and in fact `@intx/types` already owns one);
   do not duplicate it here.
3. It is not session-plane logic, HTTP-surface logic, or a database concern
   -- those have dedicated packages.
4. It depends only on node built-in modules. External dependencies signal
   that the code probably wants a more specific home.

If this package grows beyond a handful of small modules, that is a signal
to find the real boundary, not to keep adding here.

See `LAYOUT.md` for the package layout this fits into.
