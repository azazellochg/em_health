BEGIN;
SELECT plan(39);

---------------------------
-- SCHEMA PRIVILEGES
---------------------------
SELECT schema_privs_are('public', 'grafana', ARRAY['USAGE'], 'grafana has USAGE on public');
SELECT schema_privs_are('public', 'emhealth', ARRAY['USAGE'], 'emhealth has USAGE on public');
SELECT schema_privs_are('uec', 'grafana', ARRAY['USAGE'], 'grafana has USAGE on uec');
SELECT schema_privs_are('uec', 'emhealth', ARRAY['USAGE'], 'emhealth has USAGE on uec');
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
SELECT table_privs_are('pganalyze', 'vacuum_stats', 'pganalyze', ARRAY['SELECT','INSERT','UPDATE','DELETE'], 'pganalyze can manipulate vacuum_stats');
SELECT table_privs_are('pganalyze', 'queries', 'pganalyze', ARRAY['SELECT','INSERT','UPDATE','DELETE'], 'pganalyze can manipulate queries');
SELECT table_privs_are('pganalyze', 'stat_snapshots', 'pganalyze', ARRAY['SELECT','INSERT','UPDATE','DELETE'], 'pganalyze can manipulate stat_snapshots');
SELECT table_privs_are('pganalyze', 'stat_statements', 'pganalyze', ARRAY['SELECT','INSERT','UPDATE','DELETE'], 'pganalyze can manipulate stat_statements');
SELECT table_privs_are('pganalyze', 'stat_explains', 'pganalyze', ARRAY['SELECT','INSERT','UPDATE','DELETE'], 'pganalyze can manipulate stat_explains');
SELECT table_privs_are('pganalyze', 'sys_stats', 'pganalyze', ARRAY['SELECT','INSERT','UPDATE','DELETE'], 'pganalyze can manipulate sys_stats');

SELECT table_privs_are('pganalyze', 'database_stats', 'grafana', ARRAY['SELECT'], 'grafana can SELECT database_stats');
SELECT table_privs_are('pganalyze', 'table_stats', 'grafana', ARRAY['SELECT'], 'grafana can SELECT table_stats');
SELECT table_privs_are('pganalyze', 'index_stats', 'grafana', ARRAY['SELECT'], 'grafana can SELECT index_stats');
SELECT table_privs_are('pganalyze', 'vacuum_stats', 'grafana', ARRAY['SELECT'], 'grafana can SELECT vacuum_stats');
SELECT table_privs_are('pganalyze', 'queries', 'grafana', ARRAY['SELECT'], 'grafana can SELECT queries');
SELECT table_privs_are('pganalyze', 'stat_snapshots', 'grafana', ARRAY['SELECT'], 'grafana can SELECT stat_snapshots');
SELECT table_privs_are('pganalyze', 'stat_statements', 'grafana', ARRAY['SELECT'], 'grafana can SELECT stat_statements');
SELECT table_privs_are('pganalyze', 'stat_explains', 'grafana', ARRAY['SELECT'], 'grafana can SELECT stat_explains');
SELECT table_privs_are('pganalyze', 'sys_stats', 'grafana', ARRAY['SELECT'], 'grafana can SELECT sys_stats');

---------------------------
-- PGANALYZE FUNCTION PRIVILEGES
---------------------------
SELECT function_privs_are('pganalyze', 'get_db_stats', ARRAY['int', 'jsonb'], 'pganalyze', ARRAY['EXECUTE'], 'pganalyze can EXECUTE get_db_stats');
SELECT function_privs_are('pganalyze', 'get_table_stats', ARRAY['int', 'jsonb'], 'pganalyze', ARRAY['EXECUTE'], 'pganalyze can EXECUTE get_table_stats');
SELECT function_privs_are('pganalyze', 'get_index_stats', ARRAY['int', 'jsonb'], 'pganalyze', ARRAY['EXECUTE'], 'pganalyze can EXECUTE get_index_stats');
SELECT function_privs_are('pganalyze', 'get_stat_statements', ARRAY['int', 'jsonb'], 'pganalyze', ARRAY['EXECUTE'], 'pganalyze can EXECUTE get_stat_statements');
SELECT function_privs_are('pganalyze', 'parse_logs', ARRAY['int', 'jsonb'], 'pganalyze', ARRAY['EXECUTE'], 'pganalyze can EXECUTE parse_logs');
SELECT function_privs_are('pganalyze', 'parse_sysinfo', ARRAY['int', 'jsonb'], 'pganalyze', ARRAY['EXECUTE'], 'pganalyze can EXECUTE parse_sysinfo');
SELECT function_privs_are('pganalyze', 'purge_stats', ARRAY['int', 'jsonb'], 'pganalyze', ARRAY['EXECUTE'], 'pganalyze can EXECUTE purge_stats');

---------------------------
-- ROLE MEMBERSHIP
---------------------------
SELECT is_member_of('pg_monitor', 'pganalyze', 'pganalyze is a member of pg_monitor');
SELECT is_member_of('pg_stat_scan_tables', 'grafana', 'grafana is a member of pg_stat_scan_tables');
SELECT is_member_of('pg_read_all_stats', 'grafana', 'grafana is a member of pg_read_all_stats');

---------------------------
-- FINISH
---------------------------
SELECT * FROM finish();
ROLLBACK;
