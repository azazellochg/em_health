Changelog
=========

Version 0.1a13
^^^^^^^^^^^^^^

* refactor psycopg transactions: use autocommit=true, refactor run_query and tasks that need to complete or fail in one transaction, __exit__ does not hide the original exception anymore
* replace toolkit_experimental funcs due to all dependent views being dropped when toolkit extension is updated
* updated Docker image 835 -> 527 Mb, extensions built in parallel stages

Version 0.1a12
^^^^^^^^^^^^^^

* improve logging for systemd services: send msg to journal instead of emhealth.log
* DB performance: index cache hit ratio per interval instead of since the stats reset, WAL size panel shows the pg_wal directory sizem, SEM copies of the per-database alerts, database picker on Health checks, SEM table list on Schema statistics, WAL rates averaged per bucket, STANDBY instead of REPLICA
* postgres_fdw import (not used yet) fixed for the fdw schema: foreign tables fdw.pg_<id>_<table> with explicit columns instead of IMPORT FOREIGN SCHEMA, application_name is a server option, new events are filtered on the microscope instead of fetching the whole event_property table, and inactive parameters are imported too (their values are still in event_property). FDW code split into fdw_manager.py (base), fdw_mssql.py (UEC) and fdw_pg.py (HM events)
* migration 014: UEC foreign tables of all instruments live in one fdw schema (fdw.ms_<id>_error_notifications etc.) instead of a schema per instrument, which emhealth had no privilege to create. Rerun 'emhealth dev -d tem import-uec' after migrating
* fix import-uec FDW setup: row_estimate_method is a tds_fdw server option (user mappings only take username and password), and UEC error times are sent as UTC by SQL Server because FreeTDS drops the datetimeoffset offset (summer-time errors were 1 h late)
* migration 013, fix UEC import: databases created before the uec column rename get the uec tables recreated with the new names (they were never filled), the first run for an instrument imported no errors (no previous error time to compare with), and error definition 0 was never imported
* migration 012 for pganalyze schema changes: database_stats collects the next transaction ID and multixact ID; DB performance transaction ID panels use them instead of commits + rollbacks (read-only transactions use no transaction ID), and utilization covers all databases
* TEM/SEM dashboards read events.data per parameter (using LATERAL instead of standard JOIN)
* fix: time-series panels were grouped by raw time instead of the time bucket
* cryocycle_daily cagg groups points by cycle (start time, end time, peak duration) instead of a daily state_agg
* fix all counters_daily: first point of a day was not counted. All counter views treat only a drop below the first value of the day as a reset. Increments between days were lost
* images/day and data/day now sum all counters of the instrument per day before averaging
* fix beamtime, cryo cycles and server-off periods come that carry on over midnight
* new pressure_monthly cagg (rollup of pressure_daily), hourly/daily/monthly pressure tiers are kept forever
* enum members are identified by value (new PK), renamed members are updated on import instead of failing
* warn when parameter storage/display units change on import
* drop unused chunk skipping on events.data.param_id
* remove "_new" suffix from events constraint and sequence names, name events.data constraints in base DDL
* move dockerignore to the right place
* fix pyproject classifiers and status
* rename pg log file to keep only 1 week of logs
* git clone depth 1
* pg healthcheck via localhost not socket to make sure init-db has finished before e.g. restore
* remove POSTGRES_HOST
* load only required env vars to db container instead of the whole file
* fix pattern matching for new watchdog 7.0+
* fix CRLF for Windows task file
* fix manifest file
* static type checks with mypy and ruff
* do not mount backups dir for grafana since we only need it for busybox container during restore
* reduce max connections for roles to 50, limit to 20 per grafana datasource
* refactor work_mem calculation and increase it for create-stats
* track cagg refreshes in stat statements
* increase shm_size for pg container

Version 0.1a11
^^^^^^^^^^^^^^

* update podman images. PG 18.6
* update pgbackrest config and setup
* update uec schema column names
* implement host metrics collector, update pganalyze schema/funcs
* postgres connection limited to container host only
* change shm size for containers
* add infinity datasource and plugin for Grafana
* connection limits for all PG roles
* add pgtune.sh script instead of timescaledb-tune
* create Systemd unit files for watchdog and host metrics collector
* disconnect all roles during migration
* remove unused project deps
* new mviews in pganalyze schema
* new dashboards for health checks, IO, host and WAL metrics
* single source version for package, image, schemas
* secure code from shell and sql injections
* refactor to have one shared public.refresh_mview proc
* escape special chars in data.value_text
* add CLAUDE.md
* do not use shell=True in subprocess
* stricter backup filename matching

Version 0.1a10
^^^^^^^^^^^^^^

* remove import skip-duplicates arg, we always use COPY and ignore duplicates from data_staging table
* remove enum_values_history and related triggers
* refactor events schema to:

   - get better column padding alignment
   - store params and enums as a dict in events.configurations
   - move from INT to BIGINT for PKs
   - add is_active column to parameters and enums
   - param limits replaced with numrange columns
   - verify parameter value_type

* refactor import XML to reduce unnecessary row updates
* parse and import thresholds
* new pgTAP tests to verify import after TEM server updates
* verify current DB schema before running import and other cmds
* move migrate cmd from dev to db commands group
* remove performance test scripts
* pass RAISE output from PG to the logger
* remove pgstattuple extension, add amcheck for future use
* parse XML parameters that have no subsystem category
* add pgtune.sh script for future use

Version 0.1a8
^^^^^^^^^^^^^

* move schema version from .env
* fix image link in README
* change segmentby and orderby for events.data to improve compression
* fix update-passwd cmd for podman-compose
* split query identifier string properly
* make staging table unlogged
* fix constraints and remove some unnecessary indexes
* fix pganalyze index and tabl stats collection for hypertable chunks
* remove pganalyze.stat_snapshots
* follow mozilla sql style guide
* update all DB performance dashboards
* new alerts for DB performance

Version 0.1a7
^^^^^^^^^^^^^

* set debug=false by default
* big docs update
* increase grafana security
* update all dashboards to V2
* set explicit read permissions for GH action
* debug=false
* refactor ownership and privileges of tables and schemas
* move public tables to events schema
* set pganalyze funcs to security invoker
* remove run query, test-query cmd
* dbmanager default user is emhealth
* fix default chunk size in the .env_example
* fix passwd for perf tests

Version 0.1a6
^^^^^^^^^^^^^

* pg 18 support
* columnstore policy is now automatically set by tsdb
* index stats simplified
* fix vacuum stats parsing from logs
* bump schema
* add hierarchical caggs, remove cagg vars
* sql files reorganized
* stats retention set to 3 months
* set chunk size for data to 7 days, to be further increased
* fix column types for several caggs
* refactor multiple LAG-based queries
* add is_active column to instruments table
* refactor CLI
* add dockerignore
* track WAL usage properly
* fixes to pganalyze tables
* clean-inst to be replaced by is_active boolean, refactor command to prune old data
* increase containers security
* delete grafana_client
* add annotations for GHCR
* improve action security
* separate sql to reset db
* set clear network and container names
* remove unused GRAFANA_API_TOKEN
* backup dbs separately

Version 0.1a4
^^^^^^^^^^^^^

* rename tem_off to em_off view, add em_off_daily cagg and cagg for TEM cryo cycle periods
* new set of dashboards, views and caggs for SEM
* new SAL dashboard for Tundra
* new Vacuum & Temperature overview dashboard for TEM
* add Podman support, as an option instead of Docker
* db performance: track only top-level statements, run get_statements before everything else to populate queries table
* update tsdb extension and toolkit in the docker image
* fix datasources yaml for new grafana
* fix pgtap tests, scope all queries to the test instrument
* fix PG log rotation config var

Version 0.1a3
^^^^^^^^^^^^^

* new dashboard for acquisition software
* add BACKUP_DIR variable
* backup and restore tested. Update untested yet

Version 0.1a2
^^^^^^^^^^^^^

* add emhealth r/w user for importing data
* split init-db sql into separate files, bind mount the whole sql dir
* pass all vars from .env to timescale container
* add userid to pganalyze statements table
* update tds_fdw version
* fix GHA cache
* update screenshots
* update schema version and add the first migration
* add missing db migrate command
* pgTAP tests passing
* drop unused indexes and update some PKs
* fix epu/tomo sessions calculation and associated counters
* create separate image renderer service since the plugin is deprecated
* fix docker image labels
* add aceiot-svg-panel plugin for future use

Version 0.1a1
^^^^^^^^^^^^^

* Initial release
