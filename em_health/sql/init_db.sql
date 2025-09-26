-- create extensions --
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;
CREATE EXTENSION IF NOT EXISTS timescaledb_toolkit CASCADE;
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
CREATE EXTENSION IF NOT EXISTS pgstattuple;
CREATE EXTENSION IF NOT EXISTS pgtap;
CREATE EXTENSION IF NOT EXISTS tds_fdw;
CREATE EXTENSION IF NOT EXISTS postgres_fdw;

-- create schemas, tables and functions --
\i /sql/public/create_tables.sql
\i /sql/public/create_functions.sql

\i /sql/uec/create_tables.sql

\i /sql/pganalyze/create_tables.sql
\i /sql/pganalyze/create_functions.sql

-- set current schema version --
INSERT INTO public.schema_info (version) VALUES (2);
