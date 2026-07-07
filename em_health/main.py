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

import argparse
from dotenv import load_dotenv
from pathlib import Path
from em_health import __version__

HM_EXE = r"C:\Program Files (x86)\Thermo Scientific Health Monitor\HealthMonitorCmd.exe"


def import_cmd(args):
    from em_health.utils.import_xml import main as func
    func(args.input, args.settings, getattr(args, "nocopy", False))


def create_task_cmd(args):
    from em_health.utils.create_task import main as func
    func(args.exe, args.settings)


def watch_cmd(args):
    from em_health.utils.watcher import main as func
    func(args.input, args.settings, args.interval)


def update_cmd(args):
    from em_health.utils.maintenance import main as func
    func("update")


def test_cmd(args):
    import unittest
    from em_health.tests.test_app import TestEMHealth
    suite = unittest.TestLoader().loadTestsFromTestCase(TestEMHealth)
    unittest.TextTestRunner(verbosity=2).run(suite)


def db_cmd(args):
    dbname = args.database
    action = args.action

    if action in ["create-stats", "erase", "prune"]:
        from em_health.db_manager import main as func
        func(dbname, action, getattr(args, "days", None))

    elif action in ["backup", "restore"]:
        from em_health.utils.maintenance import main as func
        func(action, dbname)


def dev_cmd(args):
    dbname = args.database
    action = args.action

    if action == "pganalyze":
        from em_health.db_analyze import main as func
        func(dbname, action, getattr(args, "force", False))

    elif action in ["import-uec", "migrate"]:
        from em_health.db_manager import main as func
        func(dbname, action)

    elif action.startswith("test-"):
        from em_health.tests.test_performance import TestPerformance
        TestPerformance(action, args.batch).run()


COMMAND_DISPATCH = {
    "import": import_cmd,
    "create-task": create_task_cmd,
    "watch": watch_cmd,
    "update": update_cmd,
    "test": test_cmd,
    "db": db_cmd,
    "dev": dev_cmd
}


def main():
    env_path = Path(__file__).resolve().parents[1] / "docker" / ".env"
    if not env_path.exists():
        raise FileNotFoundError(f"Environment file {env_path} not found")
    load_dotenv(dotenv_path=env_path)

    parser = argparse.ArgumentParser(
        prog="emhealth",
        description=f"EMHealth CLI (v{__version__})",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument("-v", "--version", action="version", version=f"EMHealth v{__version__}")

    subparsers = parser.add_subparsers(dest="command", required=True)

    # --- Import command ---
    import_parser = subparsers.add_parser("import", help="Import health monitor data from XML file")
    import_parser.add_argument("-i", dest="input", required=True,
                               help="Path to XML file (.xml or .xml.gz)")
    import_parser.add_argument("-s", dest="settings", required=True,
                               help="Path to instruments.json with metadata")
    import_parser.add_argument("--skip-duplicates", dest="nocopy", action="store_true",
                               help="Ignore duplicated datapoints (useful for small overlapping imports)")

    # --- Create Task command ---
    task_parser = subparsers.add_parser("create-task",
                                        help="Create a Windows batch file to export Health Monitor data")
    task_parser.add_argument("-e", dest="exe", type=str, default=HM_EXE,
                             help=f"Custom path to the Health Monitor executable (default: '{HM_EXE}')")
    task_parser.add_argument("-s", dest="settings", required=True,
                             help="Path to instruments.json with metadata")

    # --- Watch command ---
    watch_parser = subparsers.add_parser("watch",
                                         help="Watch directory for XML file changes and trigger import")
    watch_parser.add_argument("-i", dest="input", required=True,
                              help="Target directory with XML data files")
    watch_parser.add_argument("-s", dest="settings", required=True,
                              help="Path to instruments.json with metadata")
    watch_parser.add_argument("-t", dest="interval", type=int, default=300,
                              help="Polling time interval in seconds (default: 300)")

    subparsers.add_parser("update", help="Update EMHealth to the latest version")
    subparsers.add_parser("test", help="Run unit tests to check the parser and import functions")

    # --- Database commands ---
    db_parser = subparsers.add_parser("db", help="Database operations")
    db_parser.add_argument("-d", dest="database", default="tem",
                           help="Database name (default: tem)")
    db_subparsers = db_parser.add_subparsers(dest="action", required=True)

    db_subparsers.add_parser("create-stats", help="Create data statistics")
    db_subparsers.add_parser("backup", help="Back up the database")
    db_subparsers.add_parser("restore", help="Restore database from backup")
    db_subparsers.add_parser("erase", help="Erase ALL data in the database")

    clean_parser = db_subparsers.add_parser("prune", help="Prune old data in the database")
    clean_parser.add_argument("--days", dest="days", type=int, required=True,
                                   help="Retain data newer than X days for ALL instruments")

    # --- Developer tools ---
    dev_parser = subparsers.add_parser("dev", help="Developer tools")
    dev_parser.add_argument("-d", dest="database", default="tem",
                            help="Database name (default: tem)")
    dev_subparsers = dev_parser.add_subparsers(dest="action", required=True)

    perf = dev_subparsers.add_parser("pganalyze", help="Reset DB performance statistics")
    perf.add_argument("-f", "--force", dest="force", action="store_true",
                      help="Erase existing pganalyze data and recreate tables")

    dev_subparsers.add_parser("migrate", help="Migrate TimescaleDB to the latest schema")
    dev_subparsers.add_parser("import-uec", help="Import UEC data from microscope servers")

    # helper function to add "batch" argument
    def add_count_arg(p):
        p.add_argument(
            "batch",
            type=int,
            help="Batch/chunk/rows size"
        )
        return p

    add_count_arg(dev_subparsers.add_parser("test-data", help="Generate CSV with simulated data"))
    add_count_arg(dev_subparsers.add_parser("test-copy", help="Benchmark COPY performance"))
    add_count_arg(dev_subparsers.add_parser("test-execmany", help="Benchmark EXECUTEMANY performance"))
    add_count_arg(dev_subparsers.add_parser("test-unnest", help="Benchmark INSERT UNNEST performance"))
    add_count_arg(dev_subparsers.add_parser("test-import", help="Benchmark XML import performance"))
    add_count_arg(dev_subparsers.add_parser("test-query", help="Benchmark query execution performance"))

    args = parser.parse_args()

    if args.command in COMMAND_DISPATCH:
        COMMAND_DISPATCH[args.command](args)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
