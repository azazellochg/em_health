BEGIN;
SELECT plan(14);

SELECT function_privs_are(
  'pg_read_file'::name,
   ARRAY['TEXT', 'BIGINT', 'BIGINT'],
  'pganalyze',
  ARRAY['EXECUTE']);

SELECT has_function('events'::name, 'purge_old_chunks'::name);
SELECT has_function('events'::name, 'import_instrument'::name);
SELECT has_function('events'::name, 'update_thresholds'::name);
SELECT has_function('events'::name, 'delete_instrument'::name);
SELECT has_function('events'::name, 'parameters_log_after_update'::name);

SELECT has_trigger('events'::name, 'parameters'::name, 'parameters_log_after_update'::name);

SELECT functions_are('pganalyze'::name, ARRAY[
  'get_db_stats',
  'get_table_stats',
  'get_index_stats',
  'get_stat_statements',
  'parse_logs',
  'import_sysinfo',
  'import_sysstats',
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

SELECT * FROM finish();
ROLLBACK;
