BEGIN;
SELECT plan(8);

CREATE TEMP TABLE test_ids (
  instrument_id BIGINT,
  config_id BIGINT,
  enum_id BIGINT
);

INSERT INTO test_ids (instrument_id, config_id)
SELECT i.id, c.id
FROM events.instruments i
JOIN events.configurations c ON c.id = i.current_config_id
WHERE i.serial = 9999;

UPDATE test_ids
SET enum_id = (
  SELECT enum_id
  FROM events.enum_types
  WHERE instrument_id = test_ids.instrument_id
    AND name = 'CameraInsertStatus_enum'
);

SELECT is(
  (SELECT value
   FROM events.enum_values
   WHERE instrument_id = (SELECT instrument_id FROM test_ids LIMIT 1)
     AND enum_id = (SELECT enum_id FROM test_ids LIMIT 1)
     AND member_name = 'NewMember'),
  11,
  'New enum value exists'
       );

SELECT is(
  (SELECT COUNT(*)
   FROM events.enum_values
   WHERE instrument_id = (SELECT instrument_id FROM test_ids LIMIT 1)
     AND enum_id = (SELECT enum_id FROM test_ids LIMIT 1)),
  12::BIGINT,
  'Number of enum values increased'
       );

SELECT is(
  (SELECT upper(abs_limits)
   FROM events.parameters
   WHERE instrument_id = (SELECT instrument_id FROM test_ids LIMIT 1)
     AND param_id = 363),
  9999::numeric,
  'Max abs_limit changed, new value validated'
       );

SELECT is(
  (SELECT is_active
   FROM events.parameters
   WHERE instrument_id = (SELECT instrument_id FROM test_ids LIMIT 1)
     AND param_id = 350),
  FALSE,
  'Param 350 was removed'
       );

SELECT is(
  (SELECT config_id
   FROM events.parameters
   WHERE instrument_id = (SELECT instrument_id FROM test_ids LIMIT 1)
     AND param_id = 363),
  (SELECT config_id FROM test_ids LIMIT 1),
  'Param 363 has new config id'
       );

SELECT is(
  (SELECT upper(abs_limits)
   FROM events.parameters_history
   WHERE instrument_id = (SELECT instrument_id FROM test_ids LIMIT 1)
     AND param_id = 363),
  10000::numeric,
  'Old max abs_limit saved'
       );

SELECT is(
  (SELECT COUNT(*)
   FROM events.parameters_history
   WHERE instrument_id = (SELECT instrument_id FROM test_ids LIMIT 1)),
  1::BIGINT,
  'Params history has 1 record'
       );

SELECT is(
  (SELECT COUNT(*)
   FROM events.configurations
   WHERE instrument_id = (SELECT instrument_id FROM test_ids LIMIT 1)),
  2::BIGINT,
  'Total 2 configuration records'
       );

SELECT events.delete_instrument((SELECT instrument_id FROM test_ids LIMIT 1));

SELECT * FROM finish();
