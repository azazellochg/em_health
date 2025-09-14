BEGIN;
SELECT plan(10);

-- Create temp tables
CREATE TEMP TABLE tmp_inst AS SELECT id::int FROM (SELECT 0 AS id LIMIT 0) sub;
CREATE TEMP TABLE tmp_enum AS SELECT id::int FROM (SELECT 0 AS id LIMIT 0) sub;

-- Insert a dummy instrument and store ID
WITH ins AS (
    INSERT INTO public.instruments (instrument, serial, model, name, template)
    VALUES ('9999, Test Instrument', 9999, 'Test instrument', 'Test', 'krios')
    RETURNING id
)
INSERT INTO tmp_inst
SELECT id FROM ins;

-- ENUM VALUES upsert
WITH et AS (
    INSERT INTO public.enum_types (instrument_id, name)
    SELECT id, 'VacuumState_enum' FROM tmp_inst
    RETURNING id
)
INSERT INTO tmp_enum
SELECT id FROM et;

INSERT INTO public.enum_values (enum_id, member_name, value)
SELECT id, 'AllVacuumColumnValvesClosed', 6 FROM tmp_enum;

INSERT INTO public.enum_values (enum_id, member_name, value)
SELECT id, 'AllVacuumColumnValvesOpened', 5 FROM tmp_enum;

SELECT results_eq($$SELECT value FROM public.enum_values WHERE member_name='AllVacuumColumnValvesClosed'$$, ARRAY[6], 'enum_values upsert works');

-- ENUM VALUES history logging
UPDATE public.enum_values SET value = 30 WHERE member_name='AllVacuumColumnValvesClosed';
SELECT results_eq($$SELECT value FROM public.enum_values_history ORDER BY inserted DESC LIMIT 1$$, ARRAY[6], 'enum_values_log_after_update works');

-- PARAMETERS upsert
INSERT INTO public.parameters (instrument_id, param_id, subsystem, component, param_name, display_name, value_type, event_id, event_name)
SELECT id, 1, 'sys', 'comp', 'p1', 'Param1', 'float', 101, 'ev1' FROM tmp_inst;

INSERT INTO public.parameters (instrument_id, param_id, subsystem, component, param_name, display_name, value_type, event_id, event_name)
SELECT id, 1, 'sys', 'comp', 'p1', 'Param1', 'int', 101, 'ev1' FROM tmp_inst;

SELECT results_eq($$SELECT value_type FROM public.parameters WHERE param_id=1$$, ARRAY['int'], 'parameters_upsert works');

-- PARAMETERS history logging
SELECT results_eq($$SELECT value_type FROM public.parameters_history WHERE param_id=1 ORDER BY inserted DESC LIMIT 1$$, ARRAY['float'], 'parameters_log_after_update works');

-- CASCADE delete from instruments → parameters removed
DELETE FROM public.instruments WHERE id IN (SELECT id FROM tmp_inst);
SELECT is_empty('SELECT * FROM public.parameters', 'parameters cascade delete works');

-- UEC relationships
INSERT INTO uec.device_type VALUES (1, 'DT1');
INSERT INTO uec.device_instance VALUES (10, 1, 'InstA');
INSERT INTO uec.error_code VALUES (1, 100, 'ERR_A');
INSERT INTO uec.subsystem VALUES (5, 'SubsystemA');
INSERT INTO uec.error_definitions VALUES (42, 5, 1, 100, 10);
INSERT INTO public.instruments (instrument, serial, model, name, template) VALUES ('instY', 1000, 'm2', 'Instrument Y', 'tmpl');
INSERT INTO uec.errors VALUES (now(), (SELECT id FROM public.instruments WHERE instrument='instY'), 42, 'Error text');

-- Verify one error inserted
SELECT results_eq($$SELECT COUNT(*)::int FROM uec.errors$$, ARRAY[1], 'Inserted one error with FK relations intact');

-- Cascade delete error_definitions → errors should cascade
DELETE FROM uec.error_definitions WHERE ErrorDefinitionID=42;
SELECT is_empty('SELECT * FROM uec.errors', 'errors cascade delete works');

-- === PGANALYZE FUNCTIONS ===
SELECT pganalyze.get_db_stats();
SELECT isnt_empty('SELECT * FROM pganalyze.database_stats', 'get_db_stats inserts row');

SELECT pganalyze.get_table_stats();
SELECT isnt_empty('SELECT * FROM pganalyze.table_stats', 'get_table_stats inserts row');

SELECT pganalyze.get_index_stats();
SELECT isnt_empty('SELECT * FROM pganalyze.index_stats', 'get_index_stats inserts row');

SELECT * FROM finish();
ROLLBACK;
