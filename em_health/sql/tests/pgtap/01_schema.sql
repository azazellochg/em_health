BEGIN;
SELECT plan(36);

---------------------------
-- EXTENSION TESTS
---------------------------
SELECT has_extension('timescaledb');
SELECT has_extension('timescaledb_toolkit');
SELECT has_extension('pg_stat_statements');
SELECT has_extension('pgstattuple');
SELECT has_extension('pgtap');
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
SELECT has_table('public', 'schema_info');
SELECT has_table('events', 'instruments');
SELECT has_table('events', 'enum_types');
SELECT has_table('events', 'enum_values');
SELECT has_table('events', 'enum_values_history');
SELECT has_table('events', 'parameters');
SELECT has_table('events', 'parameters_history');
SELECT has_table('events', 'data_staging');
SELECT has_table('events', 'data');

-- EVENTS INDEXES
SELECT has_index('events', 'enum_values', 'enum_values_member_name_enum_id_idx');
SELECT has_index('events', 'parameters', 'parameters_enum_id_instrument_id_param_id_param_name_subsys_idx');

---------------------------
-- UEC SCHEMA TABLES
---------------------------
SELECT has_table('uec', 'device_type');
SELECT has_table('uec', 'device_instance');
SELECT has_table('uec', 'error_code');
SELECT has_table('uec', 'subsystem');
SELECT has_table('uec', 'error_definitions');
SELECT has_table('uec', 'errors');

---------------------------
-- PGANALYZE SCHEMA TABLES
---------------------------
SELECT has_table('pganalyze', 'database_stats');
SELECT has_table('pganalyze', 'table_stats');
SELECT has_table('pganalyze', 'index_stats');
SELECT has_table('pganalyze', 'vacuum_stats');
SELECT has_table('pganalyze', 'queries');
SELECT has_table('pganalyze', 'stat_snapshots');
SELECT has_table('pganalyze', 'stat_statements');
SELECT has_table('pganalyze', 'stat_explains');
SELECT has_table('pganalyze', 'sys_stats');

---------------------------
-- FINISH
---------------------------
SELECT * FROM finish();
