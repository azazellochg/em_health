-- create extensions --
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

CREATE EXTENSION IF NOT EXISTS timescaledb_toolkit CASCADE;

CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

CREATE EXTENSION IF NOT EXISTS pgtap;

CREATE EXTENSION IF NOT EXISTS amcheck;

CREATE EXTENSION IF NOT EXISTS tds_fdw;

CREATE EXTENSION IF NOT EXISTS postgres_fdw;

-- public schema
CREATE TABLE IF NOT EXISTS public.schema_info (
  version INT PRIMARY KEY,
  updated timestamptz NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE public.schema_info IS 'Global schema version';

GRANT SELECT ON TABLE public.schema_info TO PUBLIC;

-- events schema
CREATE SCHEMA IF NOT EXISTS events AUTHORIZATION emhealth;

COMMENT ON SCHEMA events IS 'HM events';

SET ROLE emhealth; -- below objects are owned by emhealth
\i /sql/events/create_tables.sql
\i /sql/events/create_triggers.sql
\i /sql/events/create_functions.sql
SET ROLE postgres;

-- uec schema
CREATE SCHEMA IF NOT EXISTS uec AUTHORIZATION emhealth;

COMMENT ON SCHEMA uec IS 'Error codes';

SET ROLE emhealth; -- below objects are owned by emhealth
\i /sql/uec/create_tables.sql
SET ROLE postgres;

-- pganalyze schema
CREATE SCHEMA IF NOT EXISTS pganalyze AUTHORIZATION pganalyze;

COMMENT ON SCHEMA pganalyze IS 'DB performance statistics';

GRANT EXECUTE ON FUNCTION pg_read_file(TEXT, BIGINT, BIGINT) TO pganalyze;

SET ROLE pganalyze; -- below objects are owned by pganalyze
\i /sql/pganalyze/create_tables.sql
\i /sql/pganalyze/create_functions.sql
\i /sql/pganalyze/create_jobs.sql
SET ROLE postgres;

-- pganalyze role privileges
GRANT USAGE ON SCHEMA events, uec TO pganalyze;

GRANT SELECT ON ALL TABLES IN SCHEMA events, uec TO pganalyze;

ALTER DEFAULT PRIVILEGES FOR ROLE emhealth IN SCHEMA events, uec GRANT SELECT ON TABLES TO pganalyze;

-- grafana role privileges
GRANT USAGE ON SCHEMA events, uec, pganalyze TO grafana;

GRANT SELECT ON ALL TABLES IN SCHEMA events, uec, pganalyze TO grafana;

ALTER DEFAULT PRIVILEGES FOR ROLE emhealth IN SCHEMA events, uec GRANT SELECT ON TABLES TO grafana;

ALTER DEFAULT PRIVILEGES FOR ROLE pganalyze IN SCHEMA pganalyze GRANT SELECT ON TABLES TO grafana;

-- update search path for users
ALTER ROLE emhealth SET search_path = events,uec,public;

ALTER ROLE pganalyze SET search_path = pganalyze,public;

ALTER ROLE grafana SET search_path = events,uec,pganalyze,public;

-- set current schema version --
INSERT INTO public.schema_info (version)
VALUES (7);
