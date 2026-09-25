BEGIN;
SELECT plan(8);

CREATE TEMP TABLE test_ids (
  instrument_id BIGINT,
  enum_id BIGINT
);

SELECT is(
  (SELECT model FROM events.instruments WHERE serial = 9999),
  'Test instrument'
       );

INSERT INTO test_ids (instrument_id)
SELECT id
FROM events.instruments
WHERE serial = 9999;

UPDATE test_ids
SET enum_id = (
  SELECT enum_id
  FROM events.enum_types
  WHERE instrument_id = test_ids.instrument_id
    AND name = 'FegState_enum'
);

SELECT is(
  (SELECT value
   FROM events.enum_values
   WHERE instrument_id = (SELECT instrument_id FROM test_ids LIMIT 1)
     AND enum_id = (SELECT enum_id FROM test_ids LIMIT 1)
     AND member_name = 'Operate'),
  4
       );

SELECT is(
  (SELECT COUNT(*)
   FROM events.enum_values
   WHERE instrument_id = (SELECT instrument_id FROM test_ids LIMIT 1)
     AND enum_id = (SELECT enum_id FROM test_ids LIMIT 1)),
  8::BIGINT
       );

SELECT is(
  (SELECT COUNT(*)
   FROM events.parameters
   WHERE instrument_id = (SELECT instrument_id FROM test_ids LIMIT 1)),
  391::BIGINT
       );

SELECT is(
  (SELECT param_name
   FROM events.parameters
   WHERE instrument_id = (SELECT instrument_id FROM test_ids LIMIT 1)
     AND param_id = 184),
  'Laldwr'
       );

SELECT is(
  (SELECT enum_id
   FROM events.parameters
   WHERE instrument_id = (SELECT instrument_id FROM test_ids LIMIT 1)
     AND param_name = 'FegState'),
  (SELECT enum_id FROM test_ids LIMIT 1)
       );

SELECT is(
  (SELECT COUNT(*)
   FROM events.data
   WHERE instrument_id = (SELECT instrument_id FROM test_ids LIMIT 1)),
  1889::BIGINT
       );

SELECT is(
  (SELECT COUNT(*)
   FROM events.data
   WHERE instrument_id = (SELECT instrument_id FROM test_ids LIMIT 1)
     AND time > '2025-07-28 11:00:00+0'),
  1333::BIGINT
       );

SELECT * FROM finish();
