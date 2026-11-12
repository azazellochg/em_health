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

-- Shared refresh job for materialized views, config = {"view": "<schema>.<name>"}.
-- SECURITY INVOKER: the job runs as its owner, who can only refresh views they own.
CREATE OR REPLACE PROCEDURE public.refresh_mview(job_id INT, config JSONB)
  LANGUAGE plpgsql AS
$func$
DECLARE
  v_view REGCLASS := (config ->> 'view')::REGCLASS;
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_class WHERE oid = v_view AND relkind = 'm') THEN
    RAISE EXCEPTION 'public.refresh_mview: % is not a materialized view', config ->> 'view';
  END IF;
  EXECUTE format('REFRESH MATERIALIZED VIEW %s', v_view);
END;
$func$;

REVOKE ALL ON PROCEDURE public.refresh_mview(INT, JSONB) FROM PUBLIC;

GRANT EXECUTE ON PROCEDURE public.refresh_mview(INT, JSONB) TO emhealth, pganalyze;

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
\i /sql/uec/create_functions.sql
SET ROLE postgres;

GRANT USAGE ON FOREIGN DATA WRAPPER postgres_fdw TO emhealth;
GRANT USAGE ON FOREIGN DATA WRAPPER tds_fdw TO emhealth;

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

ALTER ROLE grafana SET statement_timeout = '5min';

-- set current schema version (passed by docker/init-db.sh and reset-db.sh: highest migrations/NNN.sql) --
INSERT INTO public.schema_info (version)
VALUES (:'var_schema_version');
