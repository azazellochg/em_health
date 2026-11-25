# EMHealth

Health monitoring for Thermo Fisher electron microscopes. Health Monitor XML exports are
imported into TimescaleDB (`tem` and `sem` databases) and shown in provisioned Grafana dashboards.
Most of the logic is in SQL (`em_health/sql/`). Python is the CLI, the importer and maintenance tooling.

## Environment
- Linux only. The supported install is `pip install -e .` from a git clone. Code finds
  `docker/.env`, `docker/compose.yaml` and `emhealth.log` relative to the repo root,
  so a non-editable install from PyPI won't run.
- `docker/.env` (copied from `docker/.env_example`) is required. `emhealth` stops if it's missing.
- Container manager is `MANAGER_TYPE` (`podman` by default, or `docker`). Containers are
  `emhealth-db`, `emhealth-grafana` and `emhealth-renderer`. Compose mounts `em_health/sql` at `/sql`.
- Python `>=3.10,<3.16`.

## Containers
- `docker/.env` fills `${VAR}` in `compose.yaml`. No container gets the whole file: each service lists
  only the variables it uses under `environment:`. If `init-db.sh` or `reset-db.sh` needs a new
  variable, add it to the `timescaledb` service too. `reset-db.sh` runs later through `exec`, so it
  only sees what the container was started with.
- The DB healthcheck uses `pg_isready -h 127.0.0.1` on purpose. During the first start, `init-db.sh`
  runs on a temporary server that only listens on the Unix socket, so a socket check reports
  ready too early (`up --wait` and `update()` depend on this).
- `BACKUP_DIR` (host) is mounted at `/backups` in the DB container only. It holds both `pg_dump`
  files and the pgBackRest repository. Grafana backup/restore goes through `podman cp` and a
  temporary busybox container.
- Postgres logs are `postgresql-%a.log` (one file per weekday, truncated on rotation), so at most 7 files.
- The image build context is `docker/`, so `docker/.dockerignore` is the one that applies.

## Common commands
```bash
podman-compose -f docker/compose.yaml up -d             # or: docker compose -f docker/compose.yaml up -d
emhealth import -i file.xml.gz -s em_health/instruments.json
emhealth db -d tem migrate                               # apply pending migrations/NNN.sql
emhealth db -d tem create-stats                          # (re)create caggs/mviews and their jobs
emhealth test                                            # see Testing below
ruff check && mypy                                       # lint and type check (pip install -e '.[dev]'), config in pyproject.toml
```

## Versioning: each version is written in one place only
| Version | Edit only | Notes |
|---|---|---|
| App | `em_health/__init__.py` (`__version__`) | `pyproject.toml` and `docs/conf.py` read it. Add a `docs/changelog.rst` entry. |
| Docker image tag | `docker/compose.yaml` (`timescaledb` image) | Independent of the app version. Bump it whenever `docker/Dockerfile` changes. The publish workflow refuses to overwrite an existing tag. |
| Image components | `ARG` lines in `docker/Dockerfile` | `PG_IMAGE` (`postgres:<version>-<distro>@sha256:<digest>`, update tag and digest together), `TS_VERSIONS` (space-separated and quoted; all are built so old backups can be restored, the last one is the default), `TLK_VERSION`, `TDS_VERSION`. CI reads these lines with `sed` for the GHCR description, so keep the `ARG NAME=value` format. |
| DB schema | the highest `em_health/sql/migrations/NNN.sql` | `maintenance.SCHEMA_VERSION`, `init-db.sh` and `reset-db.sh` all work it out from the file names. |

## Schema changes
- Add a new `em_health/sql/migrations/NNN.sql` (3 digits). Follow the existing files: check
  `current_version = NNN-1`, then end with `UPDATE public.schema_info SET version = NNN`.
- Never edit a migration that has already been released. Databases already at that version will never run it again.
- Apply the same change to the base DDL (`sql/init_db.sql`, `sql/events/`, `sql/uec/`,
  `sql/pganalyze/`). New installs get the version number without running migrations, so if the
  base DDL is missing the change, they report a version they don't have.

## SQL and database conventions
- Roles: `emhealth` owns the `events` and `uec` schemas and does imports. `pganalyze` owns the `pganalyze` schema.
  `grafana` is read-only. Use `postgres` (superuser) only for migrations and admin tasks.
- `PgClient.run_query(query, identifiers=..., strings=..., values=...)`: `identifiers` are quoted with
  `sql.Identifier`, `strings` with `sql.Literal`, and `values` are bound `%s` parameters. Use `values` for
  everything except DDL that can't take parameters (FDW server/user mapping options).
  `fetch="one"`/`"all"` returns rows.
- The connection is in autocommit mode: every statement commits right away. Statements that must succeed or fail
  together go in a `with self.conn.transaction():` block (commits at the end, rolls back on an exception).
  Never call `commit()`/`rollback()` or switch `autocommit`.
- `execute_file()` runs a whole `.sql` file in one transaction (one multi-statement message) and commits it. Variables are written
  `:'var_name'` (psql quoting). `execute_file(variables=...)` substitutes them as `sql.Literal` and raises
  on a missing one. `psql -v var_name=raw_value` (the init scripts) quotes them itself, so pass raw values.
- `init_db.sql` uses `\i /sql/...` and runs only through psql inside the container.
- Caggs live in `sql/events/cagg/` and mviews in `sql/events/mviews/`. They're registered in `db_manager.main()`.
  Their order there is the dependency order. `create-stats` drops and recreates them all, so a cagg/mview change
  needs no migration, only a changelog note to rerun `create-stats`. The name suffix (`hourly`/`daily`/`monthly`)
  picks the refresh policy.
- A cagg only sees the points of its own day, and most parameters are logged only when they change:
  - Counters: never `delta(counter_agg)` per day (the step from yesterday's last value is lost, and a drop counts
    as a reset to 0). Store per-day first/last/min/max in a `*_counter_values_daily` cagg, and compute daily
    increments in a plain view with the old name (`LAG(last_value)`), created in the same file. See
    `load_counter_values_daily.sql`.
  - States: carry the previous day's state with `interpolated_duration_in`/`interpolated_state_periods` and
    `LAG(agg)`. Periods that cross midnight are merged in an mview (`sem_cryocycles`).
  - Gaps (e.g. a missing heartbeat) are invisible to a cagg: `em_off` reads the raw heartbeat with `LEAD(time)`.
- Cagg expressions must be immutable (e.g. `EXTRACT(EPOCH FROM time - 'epoch'::timestamptz)`, not
  `EXTRACT(EPOCH FROM time)`), otherwise TimescaleDB warns.
- Scheduled jobs use static procedures with a `config` JSONB, never generated per-object code:
  `public.refresh_mview` (`{"view": "schema.name"}`) for mviews and `uec.import_errors`
  (`{"instrument_id"}`) for UEC import, one job per instrument.
- Foreign tables live in the `fdw` schema (owned by `emhealth`, no grants to `grafana`/`pganalyze`),
  named `<server>_<table>`, e.g. `fdw.ms_5_error_notifications`. tds_fdw maps one foreign server to one
  microscope, so each instrument has its own tables. With the `query` option tds_fdw matches columns by
  exact name: alias them in lower case. `fdw_manager.ForeignServer` is the base class; `fdw_mssql.MssqlFdw`
  (tds_fdw, UEC from the SQL Server `qry` views, TEMs only) and `fdw_pg.PgFdw` (postgres_fdw, HM events from
  the microscope Postgres `core` schema) create their server and tables. Declare foreign table columns
  explicitly (no IMPORT FOREIGN SCHEMA: names would collide, and remote types like citext don't exist here).
  Pass import watermarks as values, not subqueries on local tables, so the filter runs on the microscope.

## Grafana dashboard SQL
- Dashboards are v2 JSON in `docker/grafana/provisioning/dashboards/{TEM,SEM}`; queries are `rawSql` strings.
  Edit them by replacing the JSON-encoded string, not by re-serializing the file.
- Read `events.data` per parameter: `params p CROSS JOIN LATERAL (SELECT ... FROM data d WHERE d.param_id = p.param_id
  AND d.instrument_id = $instrument AND $__timeFilter(d.time) OFFSET 0) d`. The instrument filter is needed for
  compressed-batch pruning (segmentby `instrument_id`). Don't disable nested loops to force plans.
- `GROUP BY time` after `time_bucket(...) AS time` groups by the raw `data.time` column: use `GROUP BY 1`.
- Compressed scans return rows mostly newest first: time series need an explicit `ORDER BY`.
- Match subsystem/component/param names case-insensitively (`lower()`); they differ in case between instruments.
  Field overrides that cover several labels use `byRegexp`.

## Data import
- The XML is parsed in streaming order: enumerations, then parameters, then values. Rows go through COPY
  in large text batches (`WRITE_DATA_CHUNK_SIZE`) into a temp staging table, then
  `INSERT ... ON CONFLICT DO NOTHING`. Keep the batching. Don't switch to row-by-row inserts.
- String values are plain text. `import_xml.COPY_ESCAPE` removes backslashes and turns tab, newline and CR
  into spaces. That loss is accepted.

## Testing
- `emhealth test` runs unittest plus pgTAP (`pg_prove`) inside the **running `emhealth-db` container
  against the live `tem`/`sem` databases**. `test_client` creates and deletes instrument serial
  `TEST_SERIAL` (9999). No test database is involved.
- pgTAP tests are in `em_health/sql/tests/pgtap/`. CI runs no tests.

## CI
- `publish_and_tag.yml`: every push to `master` builds an sdist, publishes it to PyPI and tags `v<version>`.
  Bump `__version__` before merging to master.
- `docker-publish.yml` (run manually) builds the image and pushes it to GHCR under the `compose.yaml` tag and `latest`.
