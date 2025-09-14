BEGIN;
SELECT plan(17);

---------------------------
-- SCHEMA PRIVILEGES
---------------------------
SELECT schema_privs_are('public', 'grafana', ARRAY['USAGE'], 'grafana has USAGE on public');
SELECT schema_privs_are('pganalyze', 'pganalyze', ARRAY['USAGE'], 'pganalyze has USAGE on pganalyze');
SELECT schema_privs_are('pganalyze', 'grafana', ARRAY['USAGE'], 'grafana has USAGE on pganalyze');

---------------------------
-- PUBLIC TABLE PRIVILEGES
---------------------------
SELECT table_privs_are('public', 'schema_info', 'grafana', ARRAY['SELECT'], 'grafana can SELECT public.schema_info');
SELECT table_privs_are('public', 'instruments', 'grafana', ARRAY['SELECT'], 'grafana can SELECT public.instruments');
SELECT table_privs_are('public', 'enum_types', 'grafana', ARRAY['SELECT'], 'grafana can SELECT public.enum_types');
SELECT table_privs_are('public', 'enum_values', 'grafana', ARRAY['SELECT'], 'grafana can SELECT public.enum_values');
SELECT table_privs_are('public', 'parameters', 'grafana', ARRAY['SELECT'], 'grafana can SELECT public.parameters');

---------------------------
-- PGANALYZE TABLE PRIVILEGES
---------------------------
SELECT table_privs_are('pganalyze', 'database_stats', 'pganalyze', ARRAY['SELECT','INSERT','UPDATE','DELETE'], 'pganalyze can manipulate database_stats');
SELECT table_privs_are('pganalyze', 'table_stats', 'pganalyze', ARRAY['SELECT','INSERT','UPDATE','DELETE'], 'pganalyze can manipulate table_stats');
SELECT table_privs_are('pganalyze', 'index_stats', 'pganalyze', ARRAY['SELECT','INSERT','UPDATE','DELETE'], 'pganalyze can manipulate index_stats');

---------------------------
-- PGANALYZE FUNCTION PRIVILEGES
---------------------------
SELECT function_privs_are('pganalyze', 'get_db_stats', ARRAY['int', 'jsonb'], 'pganalyze', ARRAY['EXECUTE'], 'pganalyze can EXECUTE get_db_stats');
SELECT function_privs_are('pganalyze', 'get_table_stats', ARRAY['int', 'jsonb'], 'pganalyze', ARRAY['EXECUTE'], 'pganalyze can EXECUTE get_table_stats');
SELECT function_privs_are('pganalyze', 'get_index_stats', ARRAY['int', 'jsonb'], 'pganalyze', ARRAY['EXECUTE'], 'pganalyze can EXECUTE get_index_stats');
SELECT function_privs_are('pganalyze', 'parse_logs', ARRAY['int', 'jsonb'], 'pganalyze', ARRAY['EXECUTE'], 'pganalyze can EXECUTE parse_logs');
SELECT function_privs_are('pganalyze', 'purge_stats', ARRAY['int', 'jsonb'], 'pganalyze', ARRAY['EXECUTE'], 'pganalyze can EXECUTE purge_stats');

---------------------------
-- ROLE MEMBERSHIP
---------------------------
SELECT is_member_of('pg_monitor', 'pganalyze', 'pganalyze is a member of pg_monitor');

---------------------------
-- FINISH
---------------------------
SELECT * FROM finish();
ROLLBACK;
