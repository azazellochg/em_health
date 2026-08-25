BEGIN;
SELECT plan(37);

---------------------------
-- SCHEMA PRIVILEGES
---------------------------
SELECT schema_privs_are('events', 'grafana', ARRAY['USAGE']);
SELECT schema_owner_is('uec', 'emhealth');
SELECT schema_privs_are('uec', 'grafana', ARRAY['USAGE']);
SELECT schema_owner_is('pganalyze', 'pganalyze');
SELECT schema_privs_are('pganalyze', 'grafana', ARRAY['USAGE']);

---------------------------
-- EVENTS TABLE PRIVILEGES
---------------------------
SELECT table_privs_are('events', 'instruments', 'grafana', ARRAY['SELECT']);
SELECT table_privs_are('events', 'enum_types', 'grafana', ARRAY['SELECT']);
SELECT table_privs_are('events', 'enum_values', 'grafana', ARRAY['SELECT']);
SELECT table_privs_are('events', 'parameters', 'grafana', ARRAY['SELECT']);

---------------------------
-- PGANALYZE TABLE PRIVILEGES
---------------------------
SELECT table_owner_is('pganalyze'::name, 'database_stats'::name, 'pganalyze'::name);
SELECT table_owner_is('pganalyze'::name, 'table_stats'::name, 'pganalyze'::name);
SELECT table_owner_is('pganalyze'::name, 'index_stats'::name, 'pganalyze'::name);
SELECT table_owner_is('pganalyze'::name, 'vacuum_stats'::name, 'pganalyze'::name);
SELECT table_owner_is('pganalyze'::name, 'queries'::name, 'pganalyze'::name);
SELECT table_owner_is('pganalyze'::name, 'stat_statements'::name, 'pganalyze'::name);
SELECT table_owner_is('pganalyze'::name, 'stat_explains'::name, 'pganalyze'::name);
SELECT table_owner_is('pganalyze'::name, 'sys_stats'::name, 'pganalyze'::name);

SELECT table_privs_are('pganalyze', 'database_stats', 'grafana', ARRAY['SELECT']);
SELECT table_privs_are('pganalyze', 'table_stats', 'grafana', ARRAY['SELECT']);
SELECT table_privs_are('pganalyze', 'index_stats', 'grafana', ARRAY['SELECT']);
SELECT table_privs_are('pganalyze', 'vacuum_stats', 'grafana', ARRAY['SELECT']);
SELECT table_privs_are('pganalyze', 'queries', 'grafana', ARRAY['SELECT']);
SELECT table_privs_are('pganalyze', 'stat_statements', 'grafana', ARRAY['SELECT']);
SELECT table_privs_are('pganalyze', 'stat_explains', 'grafana', ARRAY['SELECT']);
SELECT table_privs_are('pganalyze', 'sys_stats', 'grafana', ARRAY['SELECT']);

---------------------------
-- PGANALYZE FUNCTION PRIVILEGES
---------------------------
SELECT function_privs_are('pganalyze', 'get_db_stats', ARRAY['int', 'jsonb'], 'pganalyze', ARRAY['EXECUTE']);
SELECT function_privs_are('pganalyze', 'get_table_stats', ARRAY['int', 'jsonb'], 'pganalyze', ARRAY['EXECUTE']);
SELECT function_privs_are('pganalyze', 'get_index_stats', ARRAY['int', 'jsonb'], 'pganalyze', ARRAY['EXECUTE']);
SELECT function_privs_are('pganalyze', 'get_stat_statements', ARRAY['int', 'jsonb'], 'pganalyze', ARRAY['EXECUTE']);
SELECT function_privs_are('pganalyze', 'parse_logs', ARRAY['int', 'jsonb'], 'pganalyze', ARRAY['EXECUTE']);
SELECT function_privs_are('pganalyze', 'parse_sysinfo', ARRAY['int', 'jsonb'], 'pganalyze', ARRAY['EXECUTE']);
SELECT function_privs_are('pganalyze', 'purge_stats', ARRAY['int', 'jsonb'], 'pganalyze', ARRAY['EXECUTE']);

---------------------------
-- ROLE MEMBERSHIP
---------------------------
SELECT is_member_of('pg_monitor', 'pganalyze');
SELECT is_member_of('pg_stat_scan_tables', 'grafana');
SELECT is_member_of('pg_read_all_stats', 'grafana');

---------------------------
-- FINISH
---------------------------
SELECT * FROM finish();
ROLLBACK;
