BEGIN;
SELECT plan(18);

SELECT table_privs_are('public'::name, 'schema_info'::name, 'public', ARRAY['SELECT']);

-- Schema owners
SELECT schema_owner_is('events'::name, 'emhealth');
SELECT schema_owner_is('uec'::name, 'emhealth');
SELECT schema_owner_is('pganalyze'::name, 'pganalyze');

-- Schema privs
SELECT schema_privs_are('events'::name, 'pganalyze', ARRAY['USAGE']);
SELECT schema_privs_are('events'::name, 'grafana', ARRAY['USAGE']);
SELECT schema_privs_are('events'::name, 'emhealth', ARRAY['USAGE', 'CREATE']);

SELECT schema_privs_are('uec'::name, 'pganalyze', ARRAY['USAGE']);
SELECT schema_privs_are('uec'::name, 'grafana', ARRAY['USAGE']);
SELECT schema_privs_are('uec'::name, 'emhealth', ARRAY['USAGE', 'CREATE']);

SELECT schema_privs_are('pganalyze'::name, 'pganalyze', ARRAY['USAGE', 'CREATE']);
SELECT schema_privs_are('pganalyze'::name, 'grafana', ARRAY['USAGE']);

-- Roles
SELECT has_role('grafana');
SELECT has_role('emhealth');
SELECT has_role('pganalyze');

SELECT is_member_of('pg_monitor', 'pganalyze');
SELECT is_member_of('pg_stat_scan_tables', 'grafana');
SELECT is_member_of('pg_read_all_stats', 'grafana');

SELECT * FROM finish();
