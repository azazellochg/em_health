BEGIN;
SELECT plan(15);

-- Insert a dummy instrument
INSERT INTO public.instruments (instrument, serial, model, name, template, server)
VALUES ('9999, Test Instrument', 9999, 'Test instrument', 'Test', 'krios', '127.0.0.1');

-- ENUM TYPES
INSERT INTO public.enum_types (instrument_id, name)
SELECT id, 'VacuumState_enum'
FROM public.instruments
WHERE serial = 9999;

INSERT INTO public.enum_types (instrument_id, name)
SELECT id, 'ALControler_enum'
FROM public.instruments
WHERE serial = 9999;

-- ENUM VALUES (ALController)
INSERT INTO public.enum_values (enum_id, member_name, value)
SELECT t.id, v.member_name, v.value
FROM public.enum_types t
         CROSS JOIN (VALUES
                         ('AL_COCKPIT', 1),
                         ('LowLevel_TAD', 3),
                         ('NORMAL', 0),
                         ('TAD', 2),
                         ('UNKNOWN', 4)
) AS v(member_name, value)
         JOIN public.instruments i ON i.id = t.instrument_id
WHERE t.name = 'ALControler_enum'
  AND i.serial = 9999;

-- ENUM VALUES (VacuumState)
INSERT INTO public.enum_values (enum_id, member_name, value)
SELECT t.id, v.member_name, v.value
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
         JOIN public.instruments i ON i.id = t.instrument_id
WHERE t.name = 'VacuumState_enum'
  AND i.serial = 9999;

-- Verify enum value insert
SELECT results_eq(
               $$SELECT v.value
FROM public.enum_values v
JOIN public.enum_types t ON t.id = v.enum_id
JOIN public.instruments i ON i.id = t.instrument_id
WHERE v.member_name='AllVacuumColumnValvesClosed'
AND i.serial = 9999$$,
               ARRAY[6],
               'enum_values insert works'
       );

-- ENUM VALUES history logging (scoped UPDATE)
UPDATE public.enum_values v
SET value = 30
FROM public.enum_types t
         JOIN public.instruments i ON i.id = t.instrument_id
WHERE v.enum_id = t.id
  AND v.member_name = 'AllVacuumColumnValvesClosed'
  AND i.serial = 9999;

SELECT results_eq(
               $$SELECT h.value
FROM public.enum_values_history h
JOIN public.enum_values v ON v.enum_id = h.enum_id
JOIN public.enum_types t ON t.id = v.enum_id
JOIN public.instruments i ON i.id = t.instrument_id
WHERE v.member_name='AllVacuumColumnValvesClosed'
AND i.serial = 9999
ORDER BY h.inserted DESC
LIMIT 1$$,
               ARRAY[6],
               'enum_values_log_after_update works'
       );

-- PARAMETERS upsert
INSERT INTO public.parameters
(instrument_id, param_id, subsystem, component, param_name, display_name, value_type, event_id, event_name)
SELECT id, 282, 'sys', 'comp', 'p1', 'Param1', 'float', 101, 'ev1'
FROM public.instruments
WHERE serial = 9999;

INSERT INTO public.parameters
(instrument_id, param_id, subsystem, component, param_name, display_name, value_type, event_id, event_name)
SELECT id, 282, 'sys', 'comp', 'p1', 'Param1', 'int', 101, 'ev1'
FROM public.instruments
WHERE serial = 9999;

SELECT results_eq(
               $$SELECT p.value_type
FROM public.parameters p
JOIN public.instruments i ON i.id = p.instrument_id
WHERE p.param_id = 282
AND i.serial = 9999$$,
               ARRAY['int'],
               'parameters_upsert works'
       );

-- PARAMETERS history logging
SELECT results_eq(
               $$SELECT ph.value_type
FROM public.parameters_history ph
JOIN public.parameters p ON p.param_id = ph.param_id
JOIN public.instruments i ON i.id = p.instrument_id
WHERE p.param_id = 282
AND i.serial = 9999
ORDER BY ph.inserted DESC
LIMIT 1$$,
               ARRAY['float'],
               'parameters_log_after_update works'
       );

-- CASCADE delete from instruments
DELETE FROM public.instruments
WHERE serial = 9999;

SELECT is_empty(
               $$SELECT p.*
FROM public.parameters p
JOIN public.instruments i ON i.id = p.instrument_id
WHERE i.serial = 9999$$,
               'parameters cascade delete works'
       );

-- UEC relationships
INSERT INTO uec.device_type VALUES (1, 'DT1');
INSERT INTO uec.device_instance VALUES (10, 1, 'InstA');
INSERT INTO uec.error_code VALUES (1, 100, 'ERR_A');
INSERT INTO uec.subsystem VALUES (5, 'SubsystemA');
INSERT INTO uec.error_definitions VALUES (42, 5, 1, 100, 10);

INSERT INTO public.instruments (instrument, serial, model, name, template)
VALUES ('instY', 1000, 'm2', 'Instrument Y', 'tmpl');

INSERT INTO uec.errors
VALUES (
           now(),
           (SELECT id FROM public.instruments WHERE instrument='instY'),
           42,
           'Error text'
       );

-- Verify one error inserted
SELECT results_eq(
               $$SELECT COUNT(*)::int
FROM uec.errors e
JOIN public.instruments i ON i.id = e.instrumentid
WHERE i.instrument='instY'$$,
               ARRAY[1],
               'Inserted one error with FK relations intact'
       );

-- Cascade delete
DELETE FROM uec.error_definitions
WHERE ErrorDefinitionID = 42;

SELECT is_empty(
               $$SELECT e.*
FROM uec.errors e
JOIN public.instruments i ON i.id = e.instrumentid
WHERE i.instrument='instY'$$,
               'errors cascade delete works'
       );

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