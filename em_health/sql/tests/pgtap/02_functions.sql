BEGIN;
SELECT plan(11);

SELECT function_privs_are(
  'pg_read_file'::name,
   ARRAY['TEXT', 'BIGINT', 'BIGINT'],
  'pganalyze',
  ARRAY['EXECUTE']);

SELECT functions_are('events'::name, ARRAY[
  'purge_old_chunks',
  'import_instrument',
  'delete_instrument',
  'parameters_log_after_update'
  ]);

SELECT has_trigger('events'::name, 'parameters'::name, 'parameters_log_after_update'::name);

SELECT functions_are('pganalyze'::name, ARRAY[
  'get_db_stats',
  'get_table_stats',
  'get_index_stats',
  'get_stat_statements',
  'parse_logs',
  'parse_sysinfo',
  'purge_stats'
  ]);

-- PGANALYZE functions
SELECT pganalyze.get_db_stats();
SELECT isnt_empty('SELECT * FROM pganalyze.database_stats');

SELECT pganalyze.get_table_stats();
SELECT isnt_empty('SELECT * FROM pganalyze.table_stats');

SELECT pganalyze.get_index_stats();
SELECT isnt_empty('SELECT * FROM pganalyze.index_stats');

SELECT pganalyze.get_stat_statements();
SELECT isnt_empty('SELECT * FROM pganalyze.stat_statements');
SELECT isnt_empty('SELECT * FROM pganalyze.queries');

SELECT pganalyze.parse_logs();
SELECT isnt_empty('SELECT * FROM pganalyze.vacuum_stats');

SELECT pganalyze.parse_sysinfo();
SELECT isnt_empty('SELECT * FROM pganalyze.sys_stats');

SELECT * FROM finish();
ROLLBACK;
