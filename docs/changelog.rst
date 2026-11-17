Changelog
=========

Version 0.1a12
^^^^^^^^^^^^^^

* migration 012: database_stats collects the next transaction ID and multixact ID; DB performance transaction ID panels use them instead of commits + rollbacks (read-only transactions use no transaction ID), and utilization covers all databases
* TEM dashboards read events.data per parameter (LATERAL): compressed batches of other parameters are skipped, same results
* fix TEM dashboards: time-series panels grouped by raw time instead of the time bucket
* tem_cryocycle_daily cagg groups points by cycle (start time, end time, peak duration) instead of a daily state_agg
* fix load_counters_daily: loads whose new counter value was the first point of a day were not counted, a counter drop counted as loads. It is now a view over the new load_counter_values_daily cagg: rerun create-stats
* fix image_counters_daily and data_counters_daily the same way (increments between days were lost: up to 6.7% of offloaded TB): they are now views over the new image_counter_values_daily and data_counter_values_daily caggs: rerun create-stats. All three counter views treat only a drop below the first value of the day as a reset, not a value logged with fewer decimals
* fix em_off: a server outage that crossed midnight was lost (the state at the start of a day now comes from the previous day)
* fix TEM productivity: images/day and data/day sum all counters of the instrument per day before averaging (instruments with two cameras showed half)
* counter views: a drop that is followed by growth past the first value of the day counts the growth after the drop
* fix SEM counter caggs like the TEM ones: fib_baa_counters_daily, fib_bda_counters_daily and gis_counters_daily are now views over new *_counter_values_daily caggs (increments between days were lost; a single logged 0 is ignored; GIS counters stored in hours are converted to seconds): rerun create-stats
* new sem_cryocycles mview: SEM cryo cycles merged across midnight (a cycle that crossed midnight was lost)
* fix sem_beamtime_daily: states carry over midnight, cryo cycles and server-off periods come from sem_cryocycles and em_off
* SEM dashboards read events.data per parameter (LATERAL)
* fix SEM dashboards: time-series panels grouped by raw time instead of the time bucket, cryo cycles from sem_cryocycles, loads since cryo cycle (instrument filter), autoloader load recovery uses counter increments, xT server timeline, current module status regardless of the time range, base vacuum limited to the time range (rolling median from hourly sketches), BAA daily usage (instrument filter), FIB source age and ignition ratio, IGP spikes and gas usage from counter increments, GIS usage from gis_counters_daily, instrument filter on data scans
* fix TEM dashboards: autoloader load recovery uses counter increments, SA aperture errors, Stage B settling, TEM server timeline, stage status variable, overrides, sorted time series
* epu_sessions and tomo_sessions mviews read events.data per parameter (LATERAL), refresh no longer decompresses all data
* vacuum_state_daily mview reads events.data per parameter (LATERAL)
* new tem_pressure_monthly cagg (rollup of tem_pressure_daily), hourly/daily/monthly pressure tiers are kept forever
* enum members are identified by value (new PK), renamed members are updated on import instead of failing
* warn when parameter storage/display units change on import
* drop unused chunk skipping on events.data.param_id
* remove "_new" suffix from events constraint and sequence names, name events.data constraints in base DDL

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
