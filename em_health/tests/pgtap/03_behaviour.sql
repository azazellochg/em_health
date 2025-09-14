BEGIN;
SELECT plan(20);

-- Insert a dummy instrument
INSERT INTO public.instruments (instrument, serial, model, name, template)
VALUES ('instX', 999, 'm1', 'Instrument X', 'tmpl')
RETURNING id INTO TEMP TABLE tmp_inst;

-- ENUM VALUES upsert
INSERT INTO public.enum_types (instrument_id, name)
SELECT id, 'enum1' FROM tmp_inst RETURNING id INTO TEMP TABLE tmp_enum;
INSERT INTO public.enum_values (enum_id, member_name, value)
SELECT id, 'VAL1', 10 FROM tmp_enum;
INSERT INTO public.enum_values (enum_id, member_name, value)
SELECT id, 'VAL1', 20 FROM tmp_enum;
SELECT results_eq($$SELECT value FROM public.enum_values WHERE member_name='VAL1'$$, ARRAY[20], 'enum_values upsert works');

-- ENUM VALUES history logging
UPDATE public.enum_values SET value = 30 WHERE member_name='VAL1';
SELECT results_eq($$SELECT value FROM public.enum_values_history ORDER BY inserted DESC LIMIT 1$$, ARRAY[20], 'enum_values_log_after_update works');

-- PARAMETERS upsert
INSERT INTO public.parameters (instrument_id, param_id, subsystem, component, param_name, display_name, value_type, event_id, event_name)
SELECT id, 1, 'sys', 'comp', 'p1', 'Param1', 'double', 101, 'ev1' FROM tmp_inst;
INSERT INTO public.parameters (instrument_id, param_id, subsystem, component, param_name, display_name, value_type, event_id, event_name)
SELECT id, 1, 'sys2', 'comp2', 'p1', 'Param1 updated', 'text', 102, 'ev2' FROM tmp_inst;
SELECT results_eq($$SELECT subsystem FROM public.parameters WHERE param_id=1$$, ARRAY['sys2'], 'parameters_upsert works');

-- PARAMETERS history logging
UPDATE public.parameters SET subsystem='sys3' WHERE param_id=1;
SELECT row_count(1, 'parameters_log_after_update inserted history');

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

SELECT row_count(1, 'Inserted one error with FK relations intact');

-- Cascade delete error_definitions → errors should cascade
DELETE FROM uec.error_definitions WHERE ErrorDefinitionID=42;
SELECT is_empty('SELECT * FROM uec.errors', 'errors cascade delete works');

-- === PGANALYZE FUNCTIONS ===
-- get_db_stats
PERFORM pganalyze.get_db_stats();
SELECT isnt_empty('SELECT * FROM pganalyze.database_stats', 'get_db_stats inserts row');

-- get_table_stats
PERFORM pganalyze.get_table_stats();
SELECT isnt_empty('SELECT * FROM pganalyze.table_stats', 'get_table_stats inserts row');

-- get_index_stats
PERFORM pganalyze.get_index_stats();
SELECT isnt_empty('SELECT * FROM pganalyze.index_stats', 'get_index_stats inserts row');

SELECT * FROM finish();
ROLLBACK;
