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

import argparse
from em_health import __version__

HM_EXE = r"C:\Program Files (x86)\Thermo Scientific Health Monitor\HealthMonitorCmd.exe"


def main():
    """Main entry point of the program."""
    parser = argparse.ArgumentParser(
        description=f"EM_health CLI (v{__version__}) - Manage and import health monitor data")

    parser.add_argument("-d", dest="database", default="tem",
                        help="Database name (default: tem)")

    subparsers = parser.add_subparsers(dest="command", required=True)

    # --- Import command ---
    import_parser = subparsers.add_parser("import", help="Import health monitor data from XML")
    import_parser.add_argument("-i", dest="input", required=True,
                               help="Path to XML file (.xml or .xml.gz)")
    import_parser.add_argument("-s", dest="settings", required=True,
                               help="Path to settings.json with metadata")
    import_parser.add_argument("--no-copy", dest="nocopy", action="store_true",
                               help="Do not use fast COPY method (useful for small imports with duplicates)")

    # --- Create Task command ---
    task_parser = subparsers.add_parser("create-task",
                                        help="Create a Windows batch file to export Health Monitor data")
    task_parser.add_argument("-i", dest="instrument", type=int, required=True,
                             help="Instrument serial number (must be in settings.json)")
    task_parser.add_argument("-e", dest="exe", type=str, default=HM_EXE,
                             help=f"Custom path to Health Monitor executable (default: '{HM_EXE}')")
    task_parser.add_argument("-s", dest="settings", required=True,
                             help="Path to settings.json with metadata")

    # --- Watch dir command ---
    watch_parser = subparsers.add_parser("watch",
                                         help="Watch directory for XML file changes and trigger import")
    watch_parser.add_argument("-i", dest="input", required=True,
                              help="Directory with XML data files")
    watch_parser.add_argument("-s", dest="settings", required=True,
                              help="Path to settings.json")

    # --- Database maintenance commands ---
    db_parser = subparsers.add_parser("db", help="Database operations")
    db_subparsers = db_parser.add_subparsers(dest="action", required=True)

    db_subparsers.add_parser("create-stats", help="Create aggregated statistics")
    db_subparsers.add_parser("backup", help="Back up both Postgres and Grafana databases")

    restore_parser = db_subparsers.add_parser("restore", help="Restore DB from backup")
    restore_parser.add_argument("-i", dest="input", required=True,
                                help="Input backup file (*.dump)")

    db_subparsers.add_parser("clean-all", help="Erase ALL data in the database")

    clean_inst_parser = db_subparsers.add_parser("clean-inst",
                                              help="Erase data for a specific instrument")
    clean_inst_parser.add_argument("-i", dest="instrument", type=int, required=True,
                                   help="Instrument serial number (must be in settings.json)")
    clean_inst_parser.add_argument("--date", type=str,
                                   help="Delete data older than this date (DD-MM-YYYY)")

    # --- Developer tools ---
    db_subparsers.add_parser("init-tables", help="Create tables structure in the database [DEV]")
    db_subparsers.add_parser("create-perf-stats", help="Setup DB performance measurements [DEV]")
    db_subparsers.add_parser("run-query", help="Run a custom query [DEV]")
    db_subparsers.add_parser("explain-query", help="EXPLAIN a custom query [DEV]")

    args = parser.parse_args()
    dbname = args.database

    if dbname not in ["tem", "sem"]:
        raise argparse.ArgumentTypeError("Database name must be 'tem' or 'sem'")

    # Dispatch based on subcommand
    if args.command == "import":
        from em_health.utils.import_xml import main as func
        func(args.input, args.settings, getattr(args, "nocopy", False))

    elif args.command == "create-task":
        from em_health.utils.create_task import main as func
        func(args.instrument, args.exe, args.settings)

    elif args.command == "watch":
        from em_health.utils.watcher import main as func
        func(args.input, args.settings)

    elif args.command == "db":

        if args.action in ["create-perf-stats", "run-query", "explain-query"]:
            from em_health.db_performance.db_analyze import main as func
            func(dbname, args.action)

        elif args.action in ["create-stats", "init-tables", "clean-all",
                             "clean-inst", "backup"]:
            from em_health.db_manager import main as func
            func(dbname, args.action,
                 getattr(args, "instrument", None),
                 getattr(args, "date", None))

        elif args.action == "restore":
            from em_health.db_manager import main as func
            func(dbname, args.action, fn=args.input)


if __name__ == '__main__':
    main()
