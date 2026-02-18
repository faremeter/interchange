-- Bootstrap the interchange database and service roles.
--
-- Prerequisites:
--   1. Connect to PostgreSQL as a superuser
--   2. Create the database if it doesn't exist:
--        CREATE DATABASE interchange;
--   3. Then connect to the interchange database and run this script:
--        psql -d interchange -f db/init.sql
--
-- The script uses the current_user (whoever runs the migration) for
-- ALTER DEFAULT PRIVILEGES, so it works regardless of whether the
-- migration superuser is called "postgres", "alexander", etc.

-- Create the hub application role (idempotent)
DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'interchange-hub') THEN
    CREATE ROLE "interchange-hub" LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE;
  END IF;
END
$$;

-- Let the hub role connect to this database
GRANT CONNECT ON DATABASE interchange TO "interchange-hub";

-- Let the hub role use the public schema (required to see tables)
GRANT USAGE ON SCHEMA public TO "interchange-hub";

-- Grant DML on the auth tables the hub server needs
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE
  "user",
  "session",
  "account",
  "verification"
TO "interchange-hub";

-- When the migration user creates new tables in the future,
-- automatically grant DML to the hub role so we don't have to re-run
-- this script after every migration. Uses current_user so this works
-- regardless of which superuser role runs migrations.
ALTER DEFAULT PRIVILEGES FOR ROLE current_user IN SCHEMA public
  GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO "interchange-hub";
