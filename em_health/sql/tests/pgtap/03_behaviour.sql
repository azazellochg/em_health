BEGIN;
SELECT plan(15);

-- Insert a dummy instrument
INSERT INTO public.instruments (instrument, serial, model, name, template, server)
VALUES ('9999, Test Instrument', 9999, 'Test instrument', 'Test', 'krios', '127.0.0.1');

-- ENUM TYPES & VALUES insert
INSERT INTO public.enum_types (instrument_id, name)
SELECT id, 'VacuumState_enum' FROM public.instruments;

INSERT INTO public.enum_types (instrument_id, name)
SELECT id, 'ALControler_enum' FROM public.instruments;

INSERT INTO public.enum_values (enum_id, member_name, value)
SELECT id, v.member_name, v.value
FROM public.enum_types t
         CROSS JOIN (VALUES
                         ('AL_COCKPIT', 1),
                         ('LowLevel_TAD', 3),
                         ('NORMAL', 0),
                         ('TAD', 2),
                         ('UNKNOWN', 4)
) AS v(member_name, value)
WHERE t.name = 'ALControler_enum';

INSERT INTO public.enum_values (enum_id, member_name, value)
SELECT id, v.member_name, v.value
FROM public.enum_types t
         CROSS JOIN (VALUES
                         ('AllVacuumColumnValvesClosed', 6),
                         ('AllVacuumColumnValvesOpened', 5),
                         ('AllVented', 13),
                         ('Busy', 2),
                         ('ColumnProjectionVented', 11),
                         ('ColumnVented', 8),
                         ('CryoCycle_Delay', 22),
                         ('CryoCycle_Time', 23),
                         ('Error', 3),
                         ('GunColumnVented', 10),
                         ('GunProjectionVented', 12),
                         ('GunVented', 7),
                         ('LoadingCycle', 21),
                         ('ManualLoaderLoadingCycle', 24),
                         ('Off', 1),
                         ('ProjectionVented', 9),
                         ('Recover', 4),
                         ('TMPmOnColumn', 15),
                         ('TMPmOnColumnProjectionVented', 18),
                         ('TMPmOnGun', 16),
                         ('TMPmOnGunProjectionVented', 19),
                         ('TMPmOnly', 17),
                         ('TMPpOnly', 20),
                         ('TMPsOnly', 14),
                         ('Unknown', 0)
) AS v(member_name, value)
WHERE t.name = 'VacuumState_enum';

SELECT results_eq($$SELECT value FROM public.enum_values WHERE member_name='AllVacuumColumnValvesClosed'$$, ARRAY[6], 'enum_values insert works');

-- ENUM VALUES history logging
UPDATE public.enum_values SET value = 30 WHERE member_name='AllVacuumColumnValvesClosed';
SELECT results_eq($$SELECT value FROM public.enum_values_history ORDER BY inserted DESC LIMIT 1$$, ARRAY[6], 'enum_values_log_after_update works');

-- PARAMETERS upsert
INSERT INTO public.parameters (instrument_id, param_id, subsystem, component, param_name, display_name, value_type, event_id, event_name)
SELECT id, 282, 'sys', 'comp', 'p1', 'Param1', 'float', 101, 'ev1'
FROM public.instruments;

INSERT INTO public.parameters (instrument_id, param_id, subsystem, component, param_name, display_name, value_type, event_id, event_name)
SELECT id, 282, 'sys', 'comp', 'p1', 'Param1', 'int', 101, 'ev1'
FROM public.instruments;

SELECT results_eq($$SELECT value_type FROM public.parameters WHERE param_id=282$$, ARRAY['int'], 'parameters_upsert works');

-- PARAMETERS history logging
SELECT results_eq($$SELECT value_type FROM public.parameters_history WHERE param_id=282 ORDER BY inserted DESC LIMIT 1$$, ARRAY['float'], 'parameters_log_after_update works');

-- CASCADE delete from instruments → parameters removed
DELETE FROM public.instruments WHERE serial = 9999;
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

-- PGANALYZE functions
SELECT pganalyze.get_db_stats();
SELECT isnt_empty('SELECT * FROM pganalyze.database_stats', 'get_db_stats inserts row');

SELECT pganalyze.get_table_stats();
SELECT isnt_empty('SELECT * FROM pganalyze.table_stats', 'get_table_stats inserts row');

SELECT pganalyze.get_index_stats();
SELECT isnt_empty('SELECT * FROM pganalyze.index_stats', 'get_index_stats inserts row');

SELECT pganalyze.get_stat_statements();
SELECT isnt_empty('SELECT * FROM pganalyze.stat_snapshots', 'get_stat_statements works');
SELECT isnt_empty('SELECT * FROM pganalyze.stat_statements', 'get_stat_statements works');
SELECT isnt_empty('SELECT * FROM pganalyze.queries', 'get_stat_statements works');

SELECT pganalyze.parse_logs();
SELECT isnt_empty('SELECT * FROM pganalyze.vacuum_stats', 'parse_logs->vacuum_stats works');

SELECT pganalyze.parse_sysinfo();
SELECT isnt_empty('SELECT * FROM pganalyze.sys_stats', 'parse_sysinfo->sys_stats works');

SELECT * FROM finish();
ROLLBACK;
