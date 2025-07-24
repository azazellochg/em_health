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
    parser.add_argument("-d", "--db", dest="db", default="tem",
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
                             help="Custom path to Health Monitor executable")
    task_parser.add_argument("-s", dest="settings", required=True,
                             help="Path to settings.json with metadata")

    # --- Watch dir command ---
    watch_parser = subparsers.add_parser("watch-dir",
                                         help="Watch directory for XML file changes and trigger import")
    watch_parser.add_argument("-i", dest="input", required=True,
                              help="Directory with XML data files")
    watch_parser.add_argument("-s", dest="settings", required=True,
                              help="Path to settings.json")

    # --- Database maintenance commands ---
    subparsers.add_parser("create-stats", help="Create aggregated statistics")
    subparsers.add_parser("backup", help="Create DB backup")
    subparsers.add_parser("create-tables", help="Create table structure in the database")
    subparsers.add_parser("clean-db", help="Erase ALL data in the database")

    clean_inst_parser = subparsers.add_parser("clean-inst",
                                              help="Erase all data for a specific instrument")
    clean_inst_parser.add_argument("--serial", type=int, required=True,
                                   help="Instrument serial number")
    clean_inst_parser.add_argument("--date", type=str,
                                   help="Delete data older than this date (DD-MM-YYYY)")

    # --- Performance tools ---
    subparsers.add_parser("create-perf-stats", help="Setup DB performance stats collection")
    subparsers.add_parser("run-query", help="Run a custom query")
    subparsers.add_parser("explain-query", help="EXPLAIN a custom query")

    args = parser.parse_args()
    dbname = args.db

    # Dispatch based on subcommand
    if args.command == "import":
        from em_health.utils.import_xml import main as func
        func(args.input, args.settings, getattr(args, "nocopy", False))

    elif args.command == "create-task":
        from em_health.utils.create_task import main as func
        func(args.instrument, args.exe, args.settings)

    elif args.command == "watch-dir":
        from em_health.utils.watcher import main as func
        func(args.input, args.settings)

    elif args.command in ["create-perf-stats", "run-query", "explain-query"]:
        from em_health.db_performance.db_analyze import main as func
        func(dbname, args.command)

    elif args.command in ["create-stats", "create-tables", "clean-db", "clean-inst"]:
        from em_health.db_manager import main as func
        func(dbname, args.command,
             getattr(args, "serial", None),
             getattr(args, "date", None))

    elif args.command == "backup":
        raise NotImplementedError("Backup command not implemented yet.")


if __name__ == '__main__':
    main()
