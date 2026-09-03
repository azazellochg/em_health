Changelog
=========

Version 0.1a9
^^^^^^^^^^^^^

* remove import skip-duplicates arg, we always use COPY and ignore duplicates from data_staging table
* remove enum_values_history and related triggers
* refactor events schema to:

   - get better column padding alignment
   - store params and enums as a dict in events.configurations
   - move from INT to BIGINT for PKs
   - add is_active column to parameters and enums
   - params limits replaced with numrange column
   - verify parameter value_type

* refactor import XML to reduce unnecessary row updates
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
