-- create extensions --
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;
CREATE EXTENSION IF NOT EXISTS timescaledb_toolkit CASCADE;
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
CREATE EXTENSION IF NOT EXISTS pgstattuple;
CREATE EXTENSION IF NOT EXISTS pgtap;
CREATE EXTENSION IF NOT EXISTS tds_fdw;
CREATE EXTENSION IF NOT EXISTS postgres_fdw;

-- public schema
CREATE TABLE IF NOT EXISTS public.schema_info (version int PRIMARY KEY,
                                               updated TIMESTAMPTZ NOT NULL DEFAULT NOW());
COMMENT ON TABLE public.schema_info IS 'Global schema version';

-- events schema
CREATE SCHEMA IF NOT EXISTS events AUTHORIZATION emhealth;
COMMENT ON SCHEMA events IS 'HM events';
\i /sql/events/create_tables.sql
\i /sql/events/create_triggers.sql
\i /sql/events/create_functions.sql

-- uec schema
CREATE SCHEMA IF NOT EXISTS uec AUTHORIZATION emhealth;
COMMENT ON SCHEMA uec IS 'Error codes';
\i /sql/uec/create_tables.sql

-- pganalyze schema
CREATE SCHEMA IF NOT EXISTS pganalyze AUTHORIZATION pganalyze;
COMMENT ON SCHEMA pganalyze IS 'DB performance statistics';
\i /sql/pganalyze/create_tables.sql
\i /sql/pganalyze/create_functions.sql

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
INSERT INTO public.schema_info (version) VALUES (5);
