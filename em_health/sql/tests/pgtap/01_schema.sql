BEGIN;
SELECT plan(36);

---------------------------
-- EXTENSION TESTS
---------------------------
SELECT has_extension('timescaledb', 'timescaledb extension installed');
SELECT has_extension('timescaledb_toolkit', 'timescaledb_toolkit extension installed');
SELECT has_extension('pg_stat_statements', 'pg_stat_statements extension installed');
SELECT has_extension('pgstattuple', 'pgstattuple extension installed');
SELECT has_extension('pgtap', 'pgtap extension installed');
SELECT has_extension('tds_fdw', 'tds_fdw extension installed');
SELECT has_extension('postgres_fdw', 'postgres_fdw extension installed');

---------------------------
-- SCHEMA TESTS
---------------------------
SELECT has_schema('public', 'public schema exists');
SELECT has_schema('uec', 'uec schema exists');
SELECT has_schema('pganalyze', 'pganalyze schema exists');

---------------------------
-- PUBLIC SCHEMA TABLES
---------------------------
SELECT has_table('public', 'schema_info', 'public.schema_info exists');
SELECT has_table('public', 'instruments', 'public.instruments exists');
SELECT has_table('public', 'enum_types', 'public.enum_types exists');
SELECT has_table('public', 'enum_values', 'public.enum_values exists');
SELECT has_table('public', 'enum_values_history', 'public.enum_values_history exists');
SELECT has_table('public', 'parameters', 'public.parameters exists');
SELECT has_table('public', 'parameters_history', 'public.parameters_history exists');
SELECT has_table('public', 'data_staging', 'public.data_staging exists');
SELECT has_table('public', 'data', 'public.data exists');

-- PUBLIC INDEXES
SELECT has_index('public','enum_values','enum_values_member_name_enum_id_idx','public.enum_values index exists');
SELECT has_index('public', 'parameters', 'parameters_enum_id_instrument_id_param_id_param_name_subsys_idx', 'public.parameters index exists');

---------------------------
-- UEC SCHEMA TABLES
---------------------------
SELECT has_table('uec','device_type','uec.device_type exists');
SELECT has_table('uec','device_instance','uec.device_instance exists');
SELECT has_table('uec','error_code','uec.error_code exists');
SELECT has_table('uec','subsystem','uec.subsystem exists');
SELECT has_table('uec','error_definitions','uec.error_definitions exists');
SELECT has_table('uec','errors','uec.errors exists');

---------------------------
-- PGANALYZE SCHEMA TABLES
---------------------------
SELECT has_table('pganalyze','database_stats','pganalyze.database_stats exists');
SELECT has_table('pganalyze','table_stats','pganalyze.table_stats exists');
SELECT has_table('pganalyze','index_stats','pganalyze.index_stats exists');
SELECT has_table('pganalyze','vacuum_stats','pganalyze.vacuum_stats exists');
SELECT has_table('pganalyze','queries','pganalyze.queries exists');
SELECT has_table('pganalyze','stat_snapshots','pganalyze.stat_snapshots exists');
SELECT has_table('pganalyze','stat_statements','pganalyze.stat_statements exists');
SELECT has_table('pganalyze','stat_explains','pganalyze.stat_explains exists');
SELECT has_table('pganalyze','sys_stats','pganalyze.sys_stats exists');

---------------------------
-- FINISH
---------------------------
SELECT * FROM finish();
