-- Bootstrap the interchange database roles and permissions.
--
-- Prerequisites:
--   1. Connect to PostgreSQL as a superuser
--   2. Create the database if it doesn't exist:
--        CREATE DATABASE interchange;
--   3. Then connect to the interchange database and run this script:
--        psql -d interchange -f db/init.sql
--
-- Two service roles are created:
--
--   interchange-migrate  Owns the schema. Runs migrations (DDL).
--   interchange-hub      Application role. Gets DML on all tables
--                        created by interchange-migrate.

-- Create the migration role (idempotent)
DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'interchange-migrate') THEN
    CREATE ROLE "interchange-migrate" LOGIN PASSWORD 'migrate-dev-password';
  END IF;
END
$$;

-- Create the hub application role (idempotent)
DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'interchange-hub') THEN
    CREATE ROLE "interchange-hub" LOGIN PASSWORD 'hub-dev-password';
  END IF;
END
$$;

-- Let both roles connect to this database
GRANT CONNECT ON DATABASE interchange TO "interchange-migrate";
GRANT CONNECT ON DATABASE interchange TO "interchange-hub";

-- interchange-migrate owns the schema and can create objects
GRANT ALL ON SCHEMA public TO "interchange-migrate";

-- interchange-hub can see the schema but not create objects
GRANT USAGE ON SCHEMA public TO "interchange-hub";

-- When interchange-migrate creates tables/sequences, automatically
-- grant DML to interchange-hub so we don't re-run this after every
-- migration.
ALTER DEFAULT PRIVILEGES FOR ROLE "interchange-migrate" IN SCHEMA public
  GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO "interchange-hub";
ALTER DEFAULT PRIVILEGES FOR ROLE "interchange-migrate" IN SCHEMA public
  GRANT USAGE ON SEQUENCES TO "interchange-hub";
