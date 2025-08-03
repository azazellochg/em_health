# **************************************************************************
# *
# * Authors:     Grigory Sharov (gsharov@mrc-lmb.cam.ac.uk) [1]
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
# *  e-mail address 'gsharov@mrc-lmb.cam.ac.uk'
# *
# **************************************************************************

import os
import subprocess
from datetime import datetime
from pathlib import Path

from em_health.utils.logs import logger

DOCKER_COMPOSE_FILE = "docker/compose.yaml"
PG_CONTAINER = "timescaledb"
GRAFANA_CONTAINER = "grafana"
BACKUP_VOLUME = "backups"
GRAFANA_VOLUME = "grafana-storage"
BACKUP_PATH = "/backups"


def run_command(command: str, capture_output=False, check=True):
    """Run a shell command with logging."""
    logger.debug("Running command: %s", command)
    return subprocess.run(command, shell=True, check=check,
                          capture_output=capture_output, text=True)


def update():
    """Update Docker containers and migrate db."""
    package_root = Path(__file__).resolve().parents[2]
    os.chdir(package_root)

    commands = [
        f"docker compose -f {DOCKER_COMPOSE_FILE} down",
        f"docker compose -f {DOCKER_COMPOSE_FILE} pull",
        f"docker compose -f {DOCKER_COMPOSE_FILE} up -d",
        "docker image prune -f"
    ]

    for cmd in commands:
        run_command(cmd)

    from em_health.db_manager import main as func
    func("tem", "migrate")
    func("sem", "migrate")

    logger.info("Finished updating")


def fix_volume_permissions():
    """Ensure backup volume permissions are correct."""
    run_command(f"docker run --rm -v {BACKUP_VOLUME}:{BACKUP_PATH} busybox sh -c 'chmod a+rwx {BACKUP_PATH}'")


def backup(dbname="tem"):
    """Backup TimescaleDB and Grafana."""
    fix_volume_permissions()
    timestamp = datetime.now().strftime("%d%m%Y_%H%M%S")

    pg_backup = f"{BACKUP_PATH}/pg_{dbname}_{timestamp}.dump"
    grafana_backup = f"{BACKUP_PATH}/grafana_{timestamp}.db"

    logger.info("Backing up TimescaleDB '%s' to %s", dbname, pg_backup)
    run_command(f"docker exec {PG_CONTAINER} pg_dump -Fc -d {dbname} -f {pg_backup}")

    logger.info("Backing up Grafana DB to %s", grafana_backup)
    run_command(f"docker exec {GRAFANA_CONTAINER} cp -p /var/lib/grafana/grafana.db {grafana_backup}")


def list_backups():
    """Return a list of backup files."""
    result = run_command(f"docker exec {PG_CONTAINER} ls {BACKUP_PATH}",
                         capture_output=True)
    return result.stdout.strip().splitlines()


def restore(dbname, backup_file):
    """Restore TimescaleDB or Grafana from backup file."""
    fix_volume_permissions()

    if backup_file.endswith(".db"):
        logger.info("Restoring Grafana DB from %s", backup_file)
        commands = [
            f"docker stop {GRAFANA_CONTAINER}",
            f"docker run --rm -v {GRAFANA_VOLUME}:/var/lib/grafana -v {BACKUP_VOLUME}:{BACKUP_PATH} "
            f"busybox sh -c 'cp -p {BACKUP_PATH}/{backup_file} /var/lib/grafana/grafana.db'",
            f"docker start {GRAFANA_CONTAINER}"
        ]
    else:
        logger.info("Restoring TimescaleDB '%s' from %s", dbname, backup_file)
        run_command(
            f"docker exec {PG_CONTAINER} psql -d {dbname} -c 'SELECT timescaledb_pre_restore();'"
        )
        prefix = f"docker exec {PG_CONTAINER}"
        commands = [
            f"{prefix} psql -d {dbname} -c 'SELECT timescaledb_pre_restore();'",
            f"{prefix} pg_restore -Fc -d {dbname} {BACKUP_PATH}/{backup_file}",
            f"{prefix} psql -d {dbname} -c 'SELECT timescaledb_post_restore();'"
        ]

    for cmd in commands:
        run_command(cmd)

    logger.info("Restore completed")


def main(dbname, action):
    """Run update/backup/restore interactively."""
    if action == "update":
        logger.info("We assume you have already run 'pip install em_heath'")
        confirm = input("Do you want to backup before updating? (Y/N): ").strip().lower()
        if confirm.startswith("y"):
            backup(dbname)
        else:
            logger.info("Backup skipped")
        update()

    elif action == "backup":
        backup(dbname)

    elif action == "restore":
        confirm = input("If restoring TimescaleDB, the target DB must be empty.\n"
                        "Run 'emhealth db clean-all' before restoring.\n"
                        "Type YES to continue: ")
        if confirm != "YES":
            logger.warning("Restore aborted by user.")
            return

        backups = list_backups()
        if not backups:
            logger.warning("No backups found.")
            return

        print("Available backups:")
        for i, f in enumerate(backups, 1):
            print(f"{i}. {f}")

        choice = input(f"Select a backup to restore (1-{len(backups)}): ").strip()
        if not choice.isdigit() or not (1 <= int(choice) <= len(backups)):
            logger.error("Invalid backup choice.")
            return

        restore(dbname, backups[int(choice) - 1])
