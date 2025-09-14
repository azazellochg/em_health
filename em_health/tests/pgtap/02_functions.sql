BEGIN;
SELECT plan(15);

---------------------------
-- PUBLIC TRIGGER FUNCTIONS
---------------------------
SELECT has_function('public', 'enum_values_upsert_before_insert', ARRAY[]::text[], 'enum_values_upsert_before_insert exists');
SELECT has_function('public', 'parameters_upsert_before_insert', ARRAY[]::text[], 'parameters_upsert_before_insert exists');
SELECT has_function('public', 'enum_values_log_after_update', ARRAY[]::text[], 'enum_values_log_after_update exists');
SELECT has_function('public', 'parameters_log_after_update', ARRAY[]::text[], 'parameters_log_after_update exists');

---------------------------
-- PUBLIC TRIGGERS
---------------------------
SELECT has_trigger('public', 'enum_values', 'enum_values_upsert_before_insert', 'enum_values_upsert_before_insert trigger exists');
SELECT has_trigger('public', 'parameters', 'parameters_upsert_before_insert', 'parameters_upsert_before_insert trigger exists');
SELECT has_trigger('public', 'enum_values', 'enum_values_log_after_update', 'enum_values_log_after_update trigger exists');
SELECT has_trigger('public', 'parameters', 'parameters_log_after_update', 'parameters_log_after_update trigger exists');

---------------------------
-- PGANALYZE FUNCTIONS
---------------------------
SELECT has_function('pganalyze', 'get_db_stats', ARRAY['int', 'jsonb'], 'enum_values_upsert_before_insert exists');
SELECT has_function('pganalyze', 'get_table_stats', ARRAY['int', 'jsonb'], 'parameters_upsert_before_insert exists');
SELECT has_function('pganalyze', 'get_index_stats', ARRAY['int', 'jsonb'], 'enum_values_log_after_update exists');
SELECT has_function('pganalyze', 'get_stat_statements', ARRAY['int', 'jsonb'], 'parameters_log_after_update exists');
SELECT has_function('pganalyze', 'parse_logs', ARRAY['int', 'jsonb'], 'parameters_log_after_update exists');
SELECT has_function('pganalyze', 'parse_sysinfo', ARRAY['int', 'jsonb'], 'parameters_log_after_update exists');
SELECT has_function('pganalyze', 'purge_stats', ARRAY['int', 'jsonb'], 'parameters_log_after_update exists');

---------------------------
-- FINISH
---------------------------
SELECT * FROM finish();
ROLLBACK;
