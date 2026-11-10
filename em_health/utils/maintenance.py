# **************************************************************************
# *
# * Authors:     Grigory Sharov (gsharov@mrclmb.ac.uk) [1]
# *
# * [1] MRC Laboratory of Molecular Biology (MRC-LMB)
# *
# * This program is free software; you can redistribute it and/or modify
# * it under the terms of the GNU General Public License as published by
# * the Free Software Foundation; either version 3 of the License, or
# * (at your option) any later version.
# *
# * This program is distributed in the hope that it will be useful,
# * but WITHOUT ANY WARRANTY; without even the implied warranty of
# * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# * GNU General Public License for more details.
# *
# * You should have received a copy of the GNU General Public License
# * along with this program; if not, write to the Free Software
# * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA
# * 02111-1307  USA
# *
# *  All comments concerning this program package may be sent to the
# *  e-mail address 'gsharov@mrclmb.ac.uk'
# *
# **************************************************************************

import os
import re
from datetime import datetime
import time
from pathlib import Path
from packaging.version import Version

from em_health.utils.tools import logger, run_command

MIGRATIONS_DIR = Path(__file__).resolve().parents[1] / "sql" / "migrations"
# Latest schema version = highest migrations/NNN.sql (same rule as docker/init-db.sh and reset-db.sh)
SCHEMA_VERSION = max(int(p.stem) for p in MIGRATIONS_DIR.glob("[0-9][0-9][0-9].sql"))
MANAGER = os.getenv("MANAGER_TYPE", "podman")

if MANAGER not in ("docker", "podman"):
    raise RuntimeError(f"Unsupported MANAGER_TYPE '{MANAGER}', use 'docker' or 'podman'")

COMPOSE = ["podman-compose"] if MANAGER == "podman" else ["docker", "compose"]
COMPOSE_FILE = "compose.yaml"
PG_CONTAINER = "emhealth-db"
GRAFANA_CONTAINER = "emhealth-grafana"
PG_EXEC = [MANAGER, "exec", PG_CONTAINER]
BACKUP_HOST_PATH = Path(os.getenv("BACKUP_DIR", "/backups"))

PG_BACKUP_RE = re.compile(r"pg_(?P<dbname>tem|sem)_(?P<pg_version>\d+)_(?P<ts_version>\d+(?:\.\d+)*)_\d{8}_\d{6}\.dump")
GRAFANA_BACKUP_RE = re.compile(r"grafana_\d{8}_\d{6}\.db")


def chdir_docker_dir() -> None:
    """Change working directory to em_health/docker."""
    package_root = Path(__file__).resolve().parents[2] / "docker"
    os.chdir(package_root)


def get_ts_version(dbname: str) -> str:
    """Retrieve TimescaleDB extension version from a running container."""
    result = run_command(
        [*PG_EXEC, "psql", "-X", "-At", "-d", dbname, "-c",
         "SELECT extversion FROM pg_extension WHERE extname = 'timescaledb';"],
        capture_output=True)
    return result.stdout.strip()


def get_pg_version() -> str:
    """Retrieve Postgres version from a running container."""
    result = run_command(
        [*PG_EXEC, "psql", "-X", "-At", "-d", "tem", "-c",
         "SELECT current_setting('server_version_num');"],
        capture_output=True)
    return result.stdout.strip()


def parse_pg_backup_name(fn: Path) -> re.Match:
    """Parse a TimescaleDB backup file name created by backup()."""
    match = PG_BACKUP_RE.fullmatch(fn.name)
    if match is None:
        raise ValueError(f"Not a valid TimescaleDB backup file name: {fn.name}")
    return match


def check_versions(dbname: str, fn: Path):
    """Compare backup file with server versions."""
    match = parse_pg_backup_name(fn)
    pg_version = match["pg_version"]
    ts_version = match["ts_version"]

    pg_version_server = get_pg_version()
    ts_version_server = get_ts_version(dbname)

    if pg_version != pg_version_server:
        logger.warning(f"Postgres version mismatch: server {pg_version_server}, backup {pg_version}")

    if ts_version != ts_version_server:
        logger.warning(f"Timescale version mismatch: server {ts_version_server} (db {dbname}), backup {ts_version}")


def erase_db(dbname: str, ts_version: str = "", do_init: bool = False) -> None:
    """Erase existing DB and optionally re-initialize it."""
    cmd = [*PG_EXEC, "/usr/local/bin/reset-db.sh", dbname, str(int(do_init))]
    if ts_version:
        cmd.append(ts_version)
    run_command(cmd)


def backup(dbname: str = "tem") -> Path:
    """Backup TimescaleDB or Grafana."""
    timestamp = datetime.now().strftime("%d%m%Y_%H%M%S")

    if dbname == "grafana":
        grafana_backup = BACKUP_HOST_PATH / f"grafana_{timestamp}.db"
        logger.info("Backing up Grafana DB to %s", grafana_backup.resolve())
        run_command([MANAGER, "stop", GRAFANA_CONTAINER])
        run_command([MANAGER, "cp", f"{GRAFANA_CONTAINER}:/var/lib/grafana/grafana.db", grafana_backup])
        run_command([MANAGER, "start", GRAFANA_CONTAINER])
        logger.info("Backup completed")
        return grafana_backup
    elif dbname in ["tem", "sem"]:
        pg_version = get_pg_version()
        ts_version = get_ts_version(dbname)
        pg_backup = Path("/backups") / f"pg_{dbname}_{pg_version}_{ts_version}_{timestamp}.dump"
        pg_host_backup = BACKUP_HOST_PATH / f"pg_{dbname}_{pg_version}_{ts_version}_{timestamp}.dump"
        logger.info("Backing up TimescaleDB '%s' to %s", dbname, pg_host_backup.resolve())
        run_command([*PG_EXEC, "pg_dump", "-Fc", "-d", dbname, "-f", pg_backup])
        logger.info("Backup completed")
        return pg_backup


def list_backups(dbname: str) -> list[Path]:
    """Return backup files created by backup(). Anything else is ignored."""
    backups = []

    for f in sorted(BACKUP_HOST_PATH.iterdir()):
        if f.is_symlink() or not f.is_file():
            continue

        if dbname in ("tem", "sem"):
            match = PG_BACKUP_RE.fullmatch(f.name)
            if match and match["dbname"] == dbname:
                backups.append(f)

        elif GRAFANA_BACKUP_RE.fullmatch(f.name):
            backups.append(f)

    return backups


def restore(backup_file: Path, target_db: str, is_grafana: bool = False) -> None:
    """Restore TimescaleDB or Grafana from a backup file."""
    if target_db == "grafana":
        # Grafana restore
        if not GRAFANA_BACKUP_RE.fullmatch(backup_file.name):
            raise ValueError(f"Not a valid Grafana backup file name: {backup_file.name}")
        logger.info("Restoring Grafana DB from %s", backup_file)
        commands = [
            [MANAGER, "stop", GRAFANA_CONTAINER],
            # The file name is passed to the script as $1, it is never pasted into the script text
            [MANAGER, "run", "--rm",
             "-v", "emhealth_grafana-storage:/var/lib/grafana",
             "-v", f"{BACKUP_HOST_PATH}:/backups:ro",
             "busybox", "sh", "-c",
             'cp "/backups/$1" /var/lib/grafana/grafana.db && chown 472:root /var/lib/grafana/grafana.db',
             "sh", backup_file.name],
            [MANAGER, "start", GRAFANA_CONTAINER],
        ]
        if is_grafana:
            commands.append([MANAGER, "exec", GRAFANA_CONTAINER, "grafana", "cli", "plugins", "update-all"])
        for cmd in commands:
            run_command(cmd)

    elif target_db in ["tem", "sem"]:
        # TimescaleDB restore
        match = parse_pg_backup_name(backup_file)
        dbname = match["dbname"]
        if target_db != dbname:
            raise ValueError(f"Target database name ({target_db}) does not match selected backup file: {backup_file.name}")
        check_versions(dbname, backup_file)
        ts_version = match["ts_version"]
        logger.info("Restoring TimescaleDB %s '%s' from %s", ts_version, dbname, backup_file)
        erase_db(dbname, ts_version, do_init=False)

        for cmd in [
            [*PG_EXEC, "psql", "-d", dbname, "-c", "SELECT timescaledb_pre_restore();"],
            [*PG_EXEC, "pg_restore", "-Fc", "-d", dbname, f"/backups/{backup_file.name}"],
            [*PG_EXEC, "psql", "-d", dbname, "-c", "SELECT timescaledb_post_restore(); ANALYZE;"],
        ]:
            run_command(cmd)

    else:
        raise ValueError(f"Unknown target database: {target_db}")

    logger.info("Restore completed")


def update() -> None:
    """Update everything."""
    from em_health import __version__
    if (Version(__version__) >= Version("0.1a6")) and get_pg_version().startswith("17"):
        print("EMHealth 0.1a6+ does not support PostgreSQL 17. "
              "Check documentation at https://em-health.readthedocs.io/latest/maintenance.html#updating-postgresql-from-v17-to-v18")

    confirm = input("Have you run 'git pull' from the installation folder (y/n)?")
    if not confirm.lower().startswith("y"):
        print("Aborted.")
        return

    # backup grafana and migrate db schemas
    grafana_backup = backup("grafana")
    migrate("tem")
    migrate("sem")

    # backup db
    pg_backup_tem = backup("tem")
    pg_backup_sem = backup("sem")

    # update containers
    chdir_docker_dir()

    for cmd in [
        [*COMPOSE, "-f", COMPOSE_FILE, "down", "-v"],
        [*COMPOSE, "-f", COMPOSE_FILE, "pull"],
        [*COMPOSE, "-f", COMPOSE_FILE, "up", "-d"],
        [MANAGER, "image", "prune", "-f"],
    ]:
        run_command(cmd)

    # restore backups
    time.sleep(5)
    restore(pg_backup_tem, "tem")
    restore(pg_backup_sem, "sem")
    # update extensions if possible
    for dbname in ["tem", "sem"]:
        run_command([
            *PG_EXEC, "psql", "-X", "-d", dbname, "-c",
            "ALTER EXTENSION timescaledb UPDATE;"
            "ALTER EXTENSION timescaledb_toolkit UPDATE;"
            "ALTER EXTENSION tds_fdw UPDATE;"
        ])

    restore(grafana_backup, "grafana", is_grafana=True)

    # update stats for pganalyze
    from em_health.db_analyze import main as func
    func("tem", "create-stats")
    func("sem", "create-stats")

    logger.info("Finished updating")


def migrate(dbname: str):
    """ Migrate db to the latest version. """
    latest_ver = SCHEMA_VERSION
    from em_health.db_manager import DatabaseManager as DBM

    try:
        with DBM(dbname, username="postgres", password="POSTGRES_PASSWORD") as db:
            current_ver = db.run_query("SELECT version FROM public.schema_info", mode="fetchone")
            current_ver = current_ver[0]
            logger.info("Current schema version: %s", current_ver)

            if current_ver < latest_ver:
                db.run_query("REVOKE CONNECT ON DATABASE {dbname} FROM PUBLIC", {"dbname": dbname})
                db.run_query("""
                    SELECT pg_terminate_backend(pid)
                    FROM pg_stat_activity
                    WHERE datname = %s
                      AND pid <> pg_backend_pid();
                """, values=(dbname,))

                for v in range(current_ver + 1, latest_ver + 1):
                    view_fn = db.get_path(target=f"migrations/{v:03d}.sql")
                    db.execute_file(view_fn)
                logger.info("Database schema migrated to version %s", latest_ver)

            elif current_ver == latest_ver:
                logger.info("Database schema is up-to-date")
            else:
                raise ValueError("Schema version is higher than expected")

    finally:
        with DBM(dbname, username="postgres", password="POSTGRES_PASSWORD") as db:
            db.run_query("GRANT CONNECT ON DATABASE {dbname} TO PUBLIC", {"dbname": dbname})


def check_db_schema():
    """Check database schema vs app schema."""
    from em_health.db_manager import DatabaseManager as DBM
    for dbname, schema in [("tem", SCHEMA_VERSION), ("sem", SCHEMA_VERSION)]:
        logger.info("Checking database %s schema version", dbname)
        with DBM(dbname) as db:
            current_ver = db.run_query("SELECT version FROM public.schema_info", mode="fetchone")
            current_ver = current_ver[0]
        if current_ver != schema:
            raise Exception(f"Your actual {dbname} DB schema v{current_ver} does not match "
                            f"application v{schema}\nRun 'emhealth db -d {dbname} migrate'")


def main(action: str, dbname: str = "tem") -> None:
    """Run update/backup/restore interactively."""
    if action == "update":
        update()

    elif action == "migrate":
        migrate(dbname)

    elif action == "backup":
        _ = backup(dbname)

    elif action == "restore":
        confirm = input(f"Restoring will DELETE existing {dbname} database.\nType YES to continue: ")
        if confirm != "YES":
            print("Restore aborted by user.")
            return

        backups = list_backups(dbname)
        if not backups:
            print("No backups found.")
            return

        print("Available backups:")
        for i, f in enumerate(backups, 1):
            print(f"{i}. {f}")

        choice = input(f"Select a backup to restore (1-{len(backups)}): ").strip()
        if not choice.isdigit() or not (1 <= int(choice) <= len(backups)):
            print("Invalid backup choice.")
            return

        restore(backups[int(choice) - 1], dbname)
