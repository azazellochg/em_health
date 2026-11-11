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

## Common commands
```bash
podman-compose -f docker/compose.yaml up -d             # or: docker compose -f docker/compose.yaml up -d
emhealth import -i file.xml.gz -s em_health/instruments.json
emhealth db -d tem migrate                               # apply pending migrations/NNN.sql
emhealth db -d tem create-stats                          # (re)create caggs/mviews and their jobs
emhealth test                                            # see Testing below
```

## Versioning: each version is written in one place only
| Version | Edit only | Notes |
|---|---|---|
| App | `em_health/__init__.py` (`__version__`) | `pyproject.toml` and `docs/conf.py` read it. Add a `docs/changelog.rst` entry. |
| Docker image tag | `docker/compose.yaml` (`timescaledb` image) | Independent of the app version. Bump it whenever `docker/Dockerfile` changes. The publish workflow refuses to overwrite an existing tag. |
| Image components | `ARG` lines at the top of `docker/Dockerfile` | `PG_VERSION` (must match the `PG_IMAGE` digest), `TS_VERSION`, `TS_OLD_VERSIONS`, `TLK_VERSION`, `TDS_VERSION`. Keep them as plain `ARG NAME=value` lines, because CI parses them for the GHCR description. |
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
  The default `mode="commit"` commits right away.
- `execute_file()` runs a whole `.sql` file in one transaction and commits it. Variables are written
  `:'var_name'` (psql quoting). `execute_file(variables=...)` substitutes them as `sql.Literal` and raises
  on a missing one. `psql -v var_name=raw_value` (the init scripts) quotes them itself, so pass raw values.
- `init_db.sql` uses `\i /sql/...` and runs only through psql inside the container.
- Caggs live in `sql/events/cagg/` and mviews in `sql/events/mviews/`. They're registered in `db_manager.main()`.
  Their order there is the dependency order.
- Scheduled jobs use static procedures with a `config` JSONB, never generated per-object code:
  `public.refresh_mview` (`{"view": "schema.name"}`) for mviews and `uec.import_errors`
  (`{"instrument_id", "fdw_schema"}`) for UEC import, one job per instrument.

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
