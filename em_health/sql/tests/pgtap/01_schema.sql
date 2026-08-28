BEGIN;
SELECT plan(37);

---------------------------
-- EXTENSION TESTS
---------------------------
SELECT has_extension('timescaledb');
SELECT has_extension('timescaledb_toolkit');
SELECT has_extension('pg_stat_statements');
SELECT has_extension('pgstattuple');
SELECT has_extension('pgtap');
SELECT has_extension('amcheck');
SELECT has_extension('tds_fdw');
SELECT has_extension('postgres_fdw');

---------------------------
-- SCHEMA TESTS
---------------------------
SELECT has_schema('events');
SELECT has_schema('uec');
SELECT has_schema('pganalyze');

---------------------------
-- EVENTS SCHEMA TABLES
---------------------------
SELECT has_table('public'::name, 'schema_info'::name);
SELECT has_table('events'::name, 'instruments'::name);
SELECT has_table('events'::name, 'enum_types'::name);
SELECT has_table('events'::name, 'enum_values'::name);
SELECT has_table('events'::name, 'enum_values_history'::name);
SELECT has_table('events'::name, 'parameters'::name);
SELECT has_table('events'::name, 'parameters_history'::name);
SELECT has_table('events'::name, 'data_staging'::name);
SELECT has_table('events'::name, 'data'::name);

-- EVENTS INDEXES
SELECT has_index('events'::name, 'enum_values'::name, 'enum_values_enum_id_member_name_key'::name);
SELECT has_index('events'::name, 'enum_values'::name, 'enum_values_enum_id_value_key'::name);
SELECT has_index('events'::name, 'parameters'::name, 'parameters_instrument_id_param_id_key'::name);

---------------------------
-- UEC SCHEMA TABLES
---------------------------
SELECT has_table('uec'::name, 'device_type'::name);
SELECT has_table('uec'::name, 'device_instance'::name);
SELECT has_table('uec'::name, 'error_code'::name);
SELECT has_table('uec'::name, 'subsystem'::name);
SELECT has_table('uec'::name, 'error_definitions'::name);
SELECT has_table('uec'::name, 'errors'::name);

---------------------------
-- PGANALYZE SCHEMA TABLES
---------------------------
SELECT has_table('pganalyze'::name, 'database_stats'::name);
SELECT has_table('pganalyze'::name, 'table_stats'::name);
SELECT has_table('pganalyze'::name, 'index_stats'::name);
SELECT has_table('pganalyze'::name, 'vacuum_stats'::name);
SELECT has_table('pganalyze'::name, 'queries'::name);
SELECT has_table('pganalyze'::name, 'stat_statements'::name);
SELECT has_table('pganalyze'::name, 'stat_explains'::name);
SELECT has_table('pganalyze'::name, 'sys_stats'::name);

---------------------------
-- FINISH
---------------------------
SELECT * FROM finish();
