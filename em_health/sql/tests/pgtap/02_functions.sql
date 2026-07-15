BEGIN;
SELECT plan(15);

---------------------------
-- EVENTS TRIGGER FUNCTIONS
---------------------------
SELECT has_function('events', 'enum_values_upsert_before_insert', ARRAY[]::text[]);
SELECT has_function('events', 'parameters_upsert_before_insert', ARRAY[]::text[]);
SELECT has_function('events', 'enum_values_log_after_update', ARRAY[]::text[]);
SELECT has_function('events', 'parameters_log_after_update', ARRAY[]::text[]);

---------------------------
-- EVENTS TRIGGERS
---------------------------
SELECT has_trigger('events'::name, 'enum_values'::name, 'enum_values_upsert_before_insert'::name);
SELECT has_trigger('events'::name, 'parameters'::name, 'parameters_upsert_before_insert'::name);
SELECT has_trigger('events'::name, 'enum_values'::name, 'enum_values_log_after_update'::name);
SELECT has_trigger('events'::name, 'parameters'::name, 'parameters_log_after_update'::name);

---------------------------
-- PGANALYZE FUNCTIONS
---------------------------
SELECT has_function('pganalyze', 'get_db_stats', ARRAY['int', 'jsonb']);
SELECT has_function('pganalyze', 'get_table_stats', ARRAY['int', 'jsonb']);
SELECT has_function('pganalyze', 'get_index_stats', ARRAY['int', 'jsonb']);
SELECT has_function('pganalyze', 'get_stat_statements', ARRAY['int', 'jsonb']);
SELECT has_function('pganalyze', 'parse_logs', ARRAY['int', 'jsonb']);
SELECT has_function('pganalyze', 'parse_sysinfo', ARRAY['int', 'jsonb']);
SELECT has_function('pganalyze', 'purge_stats', ARRAY['int', 'jsonb']);

---------------------------
-- FINISH
---------------------------
SELECT * FROM finish();
ROLLBACK;
