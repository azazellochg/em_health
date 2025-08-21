-- Drop schemas
DROP SCHEMA IF EXISTS uec CASCADE;
DROP SCHEMA IF EXISTS pganalyze CASCADE;

-- Drop tables
DROP TABLE IF EXISTS public.data CASCADE;
DROP TABLE IF EXISTS public.parameters CASCADE;
DROP TABLE IF EXISTS public.instruments CASCADE;
DROP TABLE IF EXISTS public.enum_types CASCADE;
DROP TABLE IF EXISTS public.enum_values CASCADE;
DROP TABLE IF EXISTS public.parameters_history CASCADE;
DROP TABLE IF EXISTS public.enum_values_history CASCADE;
DROP TABLE IF EXISTS public.schema_info CASCADE;

-- Drop functions
DROP FUNCTION IF EXISTS enum_values_upsert_before_insert;
DROP FUNCTION IF EXISTS parameters_upsert_before_insert;
DROP FUNCTION IF EXISTS enum_values_log_after_update;
DROP FUNCTION IF EXISTS parameters_log_after_update;
