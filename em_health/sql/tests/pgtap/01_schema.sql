BEGIN;
SELECT plan(9);

-- Extensions
SELECT extensions_are(ARRAY [
  'timescaledb',
  'timescaledb_toolkit',
  'pg_stat_statements',
  'plpgsql',
  'pgtap',
  'amcheck',
  'tds_fdw',
  'postgres_fdw'
  ]);

-- Schemas
SELECT has_schema('events');
SELECT has_schema('uec');
SELECT has_schema('pganalyze');

-- public.schema_info
SELECT has_table('public'::name, 'schema_info'::name);
SELECT col_not_null('schema_info'::name,'version');

-- Tables for all schemas
SELECT tables_are('events'::name, ARRAY [
  'configurations',
  'instruments',
  'enum_types',
  'enum_values',
  'parameters',
  'parameters_history',
  'data_staging',
  'data'
  ]);


SELECT tables_are('uec'::name, ARRAY [
  'device_type',
  'device_instance',
  'error_code',
  'subsystem',
  'error_definitions',
  'errors'
  ]);

SELECT tables_are('pganalyze'::name, ARRAY [
  'database_stats',
  'table_stats',
  'index_stats',
  'vacuum_stats',
  'queries',
  'stat_statements',
  'stat_explains',
  'sys_stats'
  ]);

SELECT * FROM finish();
