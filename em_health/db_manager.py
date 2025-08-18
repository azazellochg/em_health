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
import psycopg.errors
from datetime import datetime, timezone
from typing import Iterable, Optional

from em_health.db_client import PgClient
from em_health.utils.logs import logger, profile


class DatabaseManager(PgClient):
    """ Manager class to operate on existing db.
    Example usage:
        with DatabaseManager(dbname) as db:
            ...
    """
    def clean_instrument_data(self,
                              instrument_serial: int,
                              since: Optional[str] = None) -> None:
        """ Erase data for a particular instrument. """
        row = self.run_query("SELECT id FROM public.instruments WHERE serial = %s",
                             values=(instrument_serial,),
                             mode="fetchone")
        instrument_id = row[0] if row else None

        if instrument_id is None:
            logger.error("No such instrument: %d", instrument_serial)
            raise ValueError("Wrong serial number")

        if since is None:
            # cascade delete all data for the instrument
            self.run_query("DELETE FROM public.instruments WHERE id = %s",
                           values=(instrument_id,))
            logger.info("Deleted instrument %s and all associated data", instrument_serial)
        else:
            from_date = datetime.strptime(since, "%d-%m-%Y").replace(tzinfo=timezone.utc)
            self.run_query("DELETE FROM public.data WHERE instrument_id = %s AND time < %s",
                           values=(instrument_id, from_date))
            self.run_query("DELETE FROM uec.errors WHERE InstrumentId = %s AND Time < %s",
                           values=(instrument_id, from_date))
            logger.info("Deleted data older than %s for instrument %s",
                        from_date, instrument_serial)

        self.conn.commit()

    def add_instrument(self, instr_dict: dict) -> tuple[int, str]:
        """ Populate the instrument metadata table.
        :param instr_dict: input dict with microscope metadata
        :return: id and name of the instrument
        We always return id for either new or existing instrument
        """
        instrument_name = instr_dict["name"]
        row = self.run_query("""
            WITH s AS (
                SELECT id FROM instruments WHERE instrument = %s OR serial = %s
            ),
            i AS (
                INSERT INTO instruments (instrument, serial, model, name, template, server)
                SELECT %s, %s, %s, %s, %s, %s
                WHERE NOT EXISTS (SELECT 1 FROM s)
                RETURNING id
            )
            SELECT id, TRUE AS is_new FROM i
            UNION ALL
            SELECT id, FALSE AS is_new FROM s
        """, values=(
            instr_dict["instrument"],
            instr_dict["serial"],
            instr_dict["instrument"],
            instr_dict["serial"],
            instr_dict["model"],
            instrument_name,
            instr_dict["template"],
            instr_dict["server"]
        ), mode="fetchone")

        instrument_id, is_new = row

        if is_new:
            logger.info("Updated instruments table", extra={"prefix": instrument_name})
        else:
            logger.info("Instrument already exists", extra={"prefix": instrument_name})

        return instrument_id, instrument_name

    def add_enumerations(self,
                         instrument_id: int,
                         enums_dict: dict,
                         instrument_name: str) -> dict[str, int]:
        """ Populate the enumerations for TEM or SEM.
        Each enum value is stored as a separate SQL row.
        :param instrument_id: Instrument id
        :param enums_dict: input dict
        :param instrument_name: Instrument name
        :return a dict {enum_types.name: enum_types.id}
        """
        logger.info("Found %d enumerations", len(enums_dict),
                    extra={"prefix": instrument_name})

        # Batch insert enum_types
        self.cur.executemany("""
            INSERT INTO public.enum_types (instrument_id, name)
            VALUES (%s, %s)
            ON CONFLICT DO NOTHING
        """, (
            (instrument_id, enum_name)
            for enum_name in enums_dict.keys()
        ))

        # Fetch IDs for inserted enums
        rows = self.run_query("SELECT id, name FROM public.enum_types WHERE instrument_id = %s",
                              values=(instrument_id,),
                              mode="fetchall")
        enum_name_to_id = {name: eid for eid, name in rows}

        # Batch insert all enum_values
        self.cur.executemany("""
            INSERT INTO public.enum_values (enum_id, member_name, value)
            VALUES (%s, %s, %s)
        """, (
            (enum_name_to_id[enum_name], member_name, value)
            for enum_name, data in enums_dict.items()
            for member_name, value in data.items()
        ))

        row = self.run_query("SELECT COUNT(*) FROM public.enum_types WHERE instrument_id = %s",
                             values=(instrument_id,),
                             mode="fetchone")
        row_count = row[0] if row else None
        self.conn.commit()
        logger.info("Updated enum_types table (%d rows)", row_count,
                    extra={"prefix": instrument_name})

        return enum_name_to_id

    def add_parameters(self,
                       instrument_id: int,
                       params_dict: dict,
                       enums_ids: dict,
                       instrument_name: str) -> None:
        """ Populate parameters table with associated metadata.
        :param instrument_id: Instrument id
        :param params_dict: input params dict
        :param enums_ids: input enums dict
        :param instrument_name: Instrument name
        """
        logger.info("Found %d parameters", len(params_dict),
                    extra={"prefix": instrument_name})

        insert_sql = """
            INSERT INTO public.parameters (
                instrument_id, param_id,
                subsystem, component, param_name, display_name,
                display_unit, storage_unit, enum_id, value_type,
                event_id, event_name, abs_min, abs_max
            ) VALUES
            (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        # Batch inserts
        data_to_insert = [
            (
                instrument_id,
                param_id,
                p_dict["subsystem"],
                p_dict["component"],
                p_dict["param_name"],
                p_dict["display_name"],
                p_dict["display_unit"],
                p_dict["storage_unit"],
                enums_ids.get(enum) if (enum := p_dict.get("enum_name")) else None,
                p_dict["value_type"],
                p_dict["event_id"],
                p_dict["event_name"],
                p_dict["abs_min"],
                p_dict["abs_max"]
            ) for param_id, p_dict in params_dict.items()
        ]

        self.cur.executemany(insert_sql, data_to_insert)

        row = self.run_query("SELECT COUNT(*) FROM public.parameters WHERE instrument_id = %s",
                             values=(instrument_id,),
                             mode="fetchone")
        row_count = row[0] if row else None
        self.conn.commit()
        logger.info("Updated parameters table (%d rows)", row_count,
                    extra={"prefix": instrument_name})

    #@profile
    def write_data(self,
                   rows: Iterable[tuple],
                   instrument_name: str,
                   nocopy: bool = False) -> None:
        """ Write raw values to the data table using COPY and a pre-serialized text buffer.
        We do not sort input data, since:
         - for each parameter XML file has a batch of datapoints already sorted by time
         - TimescaleDB data table has chunking with compression, chunks will be sorted by time

        :param rows: Iterable of tuples
        :param instrument_name: Instrument name
        :param nocopy: If True, revert to executemany with duplicate handling
        """
        if nocopy:
            logger.info("No-copy mode. Duplicate entries are ignored.",
                        extra={"prefix": instrument_name})
            query = """
                INSERT INTO public.data (time, instrument_id, param_id, value_num, value_text)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """
            self.cur.executemany(query, rows)
            self.conn.commit()
        else:
            query = """
                COPY public.data (time, instrument_id, param_id, value_num, value_text)
                FROM STDIN WITH (FORMAT text)
            """

            def stream_chunks(rows: Iterable[tuple], max_size: int) -> Iterable[str]:
                buffer = []
                size = 0
                for row in rows:
                    newrow = "\t".join(col for col in row) + "\n"
                    size += len(newrow.encode("utf-8"))
                    buffer.append(newrow)
                    if size >= max_size:
                        yield ''.join(buffer)
                        buffer.clear()
                        size = 0
                if buffer:
                    yield ''.join(buffer)

            chunk_size = int(os.getenv("WRITE_DATA_CHUNK_SIZE", 65536))
            try:
                with self.cur.copy(query) as copy:
                    for chunk in stream_chunks(rows, chunk_size):
                        copy.write(chunk)
            except psycopg.errors.UniqueViolation as e:
                logger.error("Duplicate entries found: %s", e,
                             extra={"prefix": instrument_name})
                raise

        logger.info("Updated data table", extra={"prefix": instrument_name})

    def drop_mview(self, name: str, is_cagg: bool = False) -> None:
        """ Delete a materialized view. """
        self.run_query("DROP MATERIALIZED VIEW IF EXISTS {name} CASCADE",
                       {"name": name})
        if not is_cagg:
            # for standard mat. views we need to manually remove the job
            proc = f"refresh_{name}"
            self.run_query("""
                SELECT delete_job(job_id)
                FROM timescaledb_information.jobs
                WHERE proc_name = {proc}
            """, strings={"proc": proc})

            self.run_query("DROP PROCEDURE IF EXISTS {name}", {"name": name})
        logger.info("Dropped materialized view %s", name)

    def schedule_mview_refresh(self, name: str) -> None:
        """ Schedule a materialized view refresh. """
        proc = f"refresh_{name}"
        period = os.getenv("CAGG_REFRESH_INTERVAL", "12 h")

        self.run_query("""
            CREATE OR REPLACE PROCEDURE {proc}(
                job_id int,
                config jsonb
            )
            LANGUAGE SQL
            AS $$
              REFRESH MATERIALIZED VIEW {name};
            $$;
        """, {"proc": proc, "name": name})

        self.run_query("SELECT add_job({proc}, {period})",
                       strings={"proc": proc, "period": period})
        logger.info("Scheduled refresh for %s every %s", name, period)

    def schedule_cagg_refresh(self,
                              name: str,
                              start_offset: Optional[str] = None,
                              end_offset: Optional[str] = None,
                              interval: Optional[str] = None) -> None:
        """ Schedule a cont. aggregate refresh.
        Notes:
        1) The difference between start_offset and end_offset must be ≥ 2x bucket size.
        2) Our buckets are 1-day wide.
        3) If you want to keep data in the continuous aggregate even if it is removed
        from the underlying hypertable, you can set the start_offset to match
        the data retention policy on the source hypertable.
        4) If you set end_offset within the current time bucket, this bucket
        is excluded from materialization.

        Here we decided to cover 3 full buckets: D-4, D-3, D-2.
        We refresh every 12h since we want minimal latency before yesterday's stats are available
        """
        if start_offset is None:
            start_offset = os.getenv("CAGG_START_OFFSET", "4 days")
        if end_offset is None:
            end_offset = os.getenv("CAGG_END_OFFSET", "1 day")
        if interval is None:
            interval = os.getenv("CAGG_REFRESH_INTERVAL", "12 hours")

        self.run_query("""
            SELECT add_continuous_aggregate_policy({name},
            start_offset => INTERVAL {start_offset},
            end_offset => INTERVAL {end_offset},
            schedule_interval => INTERVAL {schedule_interval})
        """, strings={
            "name": name,
            "start_offset": start_offset,
            "end_offset": end_offset,
            "schedule_interval": interval
        })
        logger.info("Scheduled continuous aggregate refresh for %s", name)

    def force_refresh_cagg(self, name: str) -> None:
        """ Force a cont. aggregate refresh.
        The WITH NO DATA option allows the continuous aggregate to be created
        instantly, so you don't have to wait for the data to be aggregated.
        Here we aggregate all historical data that has been imported so far.
        """
        self.conn.autocommit = True  # required since CALL cannot be executed inside a transaction
        self.run_query("CALL refresh_continuous_aggregate({name}, NULL, NULL)",
                       strings={"name": name})
        self.conn.autocommit = False
        logger.info("Forced continuous aggregate refresh for %s", name)

    def enable_rt_cagg(self, name: str) -> None:
        """ Real-time aggregates automatically add the most recent data when
        you query your continuous aggregate. """
        self.run_query("ALTER MATERIALIZED VIEW {name} set (timescaledb.materialized_only = false)",
                       {"name": name})

    def create_mview(self, name: str) -> None:
        """ Create a new materialized view or a continuous aggregate. """
        if "." in name:
            schema, name = name.split(".", 1)
        else:
            schema = "public"

        view_fn = self.get_path(target=name+".sql", folder="views/" + schema)
        self.execute_file(view_fn)
        logger.info("Created materialized view %s.%s", schema, name)

    def migrate_db(self, latest_ver: int):
        """ Migrate db to the latest version. """
        current_ver = self.run_query("SELECT version FROM public.schema_info", mode="fetchone")
        current_ver = current_ver[0]
        logger.info("Current schema version: %s", current_ver)

        if current_ver < latest_ver:
            for v in range(current_ver + 1, latest_ver + 1):
                view_fn = self.get_path(target=f"{v:03d}.sql", folder="db_migrations")
                self.execute_file(view_fn,
                                  {"TBL_STATEMENTS_COMPRESSION": os.getenv("TBL_STATEMENTS_COMPRESSION")})
            logger.info("Database schema migrated to version %s", latest_ver)
        elif current_ver == latest_ver:
            logger.info("Database schema is up-to-date")
        else:
            raise ValueError("Database version is higher than expected")

    def import_uec(self):
        if any(os.getenv(var) in ["None", "", None] for var in ["MSSQL_USER", "MSSQL_PASSWORD"]):
            logger.warning("MSSQL_USER and MSSQL_PASSWORD are not set.")
            exit(0)

        servers = self.run_query("""
            SELECT id, server FROM public.instruments
            WHERE server IS NOT NULL
        """, mode="fetchall")

        if not servers:
            raise ValueError("No servers found in the public.instrument table")

        for instr_id, server in servers:
            name = f"mssql_{instr_id}"
            t_definitions = f"{name}_error_definitions"
            t_notifications = f"{name}_error_notifications"

            self.setup_fdw(str(server), name)
            self.create_fdw_tables(name, t_definitions, t_notifications)
            self.setup_import_fdw(name, instr_id, t_definitions, t_notifications)
            self.run_query(f"SELECT add_job('uec.import_from_{name}', schedule_interval=>'1 hour')")

            logger.info("Scheduled UEC import job for instrument %s", instr_id)

    def setup_fdw(self, server: str, name: str):
        """ Create a foreign data wrapper for the MSSQL database. """
        self.run_query("""
            CREATE SERVER IF NOT EXISTS {name}
            FOREIGN DATA WRAPPER tds_fdw
            OPTIONS (
                servername {server},
                port '1433',
                database 'DS'
            );

            CREATE USER MAPPING IF NOT EXISTS FOR public
            SERVER {name}
            OPTIONS (username {user}, password {password});
        """, identifiers={"name": name}, strings={
            "server": server,
            "user": os.getenv("MSSQL_USER"),
            "password": os.getenv("MSSQL_PASSWORD")
        })

    def create_fdw_tables(self, name: str, t_definitions: str, t_notifications: str):
        """ Create tables for foreign data wrapper. """
        self.run_query(f"""
            CREATE SCHEMA IF NOT EXISTS fdw;
            CREATE FOREIGN TABLE IF NOT EXISTS fdw.{t_definitions} (
                ErrorDefinitionID INTEGER,
                SubsystemID INTEGER,
                Subsystem TEXT,
                DeviceTypeID INTEGER,
                DeviceType TEXT,
                DeviceInstanceID INTEGER,
                DeviceInstance TEXT,
                ErrorCodeID INTEGER,
                ErrorCode TEXT
            ) SERVER {name}
            OPTIONS (schema_name 'qry', table_name 'ErrorDefinitions');
        """, {"t_definitions": t_definitions, "name": name})

        self.run_query(f"""
            CREATE FOREIGN TABLE IF NOT EXISTS fdw.{t_notifications} (
                ErrorDtm TIMESTAMPTZ,
                ErrorDefinitionID INTEGER,
                MessageText TEXT
            ) SERVER {name}
            OPTIONS (schema_name 'qry', table_name 'ErrorNotifications');
        """, {"t_notifications": t_notifications, "name": name})

    def setup_import_fdw(self,
                         name: str,
                         instr_id: int,
                         t_definitions: str,
                         t_notifications: str):
        """ Create a function to import data from the MS SQL database. """
        job = f"import_from_{name}"
        self.run_query(f"""
            DROP FUNCTION IF EXISTS uec.{job};
            CREATE FUNCTION uec.{job}(job_id INT DEFAULT NULL, config JSONB DEFAULT NULL)
            RETURNS void
            LANGUAGE plpgsql
            AS $$
            BEGIN
               -- 1. Device types
                INSERT INTO uec.device_type (DeviceTypeID, IdentifyingName)
                SELECT DISTINCT fdw.DeviceTypeID, fdw.DeviceType
                FROM fdw.{t_definitions} fdw
                LEFT JOIN uec.device_type dt ON dt.DeviceTypeID = fdw.DeviceTypeID
                WHERE dt.DeviceTypeID IS NULL;

                -- 2. Device instances
                INSERT INTO uec.device_instance (DeviceInstanceID, DeviceTypeID, IdentifyingName)
                SELECT DISTINCT fdw.DeviceInstanceID, fdw.DeviceTypeID, fdw.DeviceInstance
                FROM fdw.{t_definitions} fdw
                LEFT JOIN uec.device_instance di
                    ON di.DeviceInstanceID = fdw.DeviceInstanceID AND di.DeviceTypeID = fdw.DeviceTypeID
                WHERE di.DeviceInstanceID IS NULL;

                -- 3. Error codes
                INSERT INTO uec.error_code (DeviceTypeID, ErrorCodeID, IdentifyingName)
                SELECT DISTINCT fdw.DeviceTypeID, fdw.ErrorCodeID, fdw.ErrorCode
                FROM fdw.{t_definitions} fdw
                LEFT JOIN uec.error_code ec
                    ON ec.DeviceTypeID = fdw.DeviceTypeID AND ec.ErrorCodeID = fdw.ErrorCodeID
                WHERE ec.ErrorCodeID IS NULL;

                -- 4. Subsystems
                INSERT INTO uec.subsystem (SubsystemID, IdentifyingName)
                SELECT DISTINCT fdw.SubsystemID, fdw.Subsystem
                FROM fdw.{t_definitions} fdw
                LEFT JOIN uec.subsystem ss ON ss.SubsystemID = fdw.SubsystemID
                WHERE ss.SubsystemID IS NULL;

                -- 5. Error definitions
                INSERT INTO uec.error_definitions (ErrorDefinitionID, SubsystemID, DeviceTypeID, ErrorCodeID, DeviceInstanceID)
                SELECT fdw.ErrorDefinitionID, fdw.SubsystemID, fdw.DeviceTypeID, fdw.ErrorCodeID, fdw.DeviceInstanceID
                FROM fdw.{t_definitions} fdw
                LEFT JOIN uec.error_definitions ed ON ed.ErrorDefinitionID = fdw.ErrorDefinitionID
                WHERE ed.ErrorDefinitionID IS NULL;

                -- 6. Error notifications
                INSERT INTO uec.errors (Time, InstrumentID, ErrorID, MessageText)
                SELECT
                    fdw.ErrorDtm,
                    {instr_id},
                    ed.ErrorDefinitionID,
                    fdw.MessageText
                FROM fdw.{t_notifications} fdw
                JOIN uec.error_definitions ed ON ed.ErrorDefinitionID = fdw.ErrorDefinitionID
                ON CONFLICT (Time, InstrumentID, ErrorID) DO NOTHING;
            END;
            $$;
        """, {"job": job, "t_definitions": t_definitions, "t_notifications": t_notifications},
                       strings={"instr_id": instr_id})


def main(dbname, action, instrument=None, date=None):
    if action == "create-stats":
        logger.info("Running aggregation on database %s", dbname)
        mviews: dict[str, bool] = {
            # name: is_cagg
            "tem_off": False,
            "vacuum_state_daily": False,

            "epu_sessions": False,
            "tomo_sessions": False,

            "epu_acquisition_daily": False,
            "tomo_acquisition_daily": False,

            "epu_counters": False,
            "tomo_counters": False,

            "load_counters_daily": True,
            "data_counters_daily": True,
            "image_counters_daily": True,
        }

        with DatabaseManager(dbname) as db:
            for mview, is_cagg in mviews.items():
                db.drop_mview(mview)
                db.create_mview(mview)
                if is_cagg:
                    db.force_refresh_cagg(mview)
                    db.schedule_cagg_refresh(mview)
                    db.enable_rt_cagg(mview)
                else:
                    db.schedule_mview_refresh(mview)
                db.run_query("GRANT SELECT ON public.{mview} TO grafana",
                             {"mview": mview})

    elif action == "init-tables":
        logger.info("Creating new table structure for database %s", dbname)
        with DatabaseManager(dbname) as db:
            db.create_tables()

    elif action == "clean-all":
        print(f"!!! WARNING: You are about to DELETE ALL DATA from database {dbname} !!!")
        confirm = input("Type YES to continue: ")
        if confirm != "YES":
            print("Aborted.")
            return
        logger.info("Deleting ALL data from database %s", dbname)
        with DatabaseManager(dbname) as db:
            db.clean_db()

    elif action == "clean-inst":
        # verify args
        if not instrument:
            raise ValueError("-i is required for clean-inst")
        if date:
            try:
                datetime.strptime(date, "%d-%m-%Y")
            except ValueError:
                raise ValueError("Invalid date format. Use DD-MM-YYYY (e.g., 23-03-2025).")

        with DatabaseManager(dbname) as db:
            if not date:
                logger.info("Deleting data for instrument %s in %s", instrument, dbname)
                db.clean_instrument_data(instrument)
            else:
                logger.info("Deleting data since %s for instrument %s in %s", date, instrument, dbname)
                db.clean_instrument_data(instrument, since=date)

    elif action == "migrate":
        latest_ver = int(os.getenv(f"{dbname.upper()}_SCHEMA_VERSION"))
        with DatabaseManager(dbname) as db:
            db.migrate_db(latest_ver)

    elif action == "import-uec":
        with DatabaseManager(dbname) as db:
            db.import_uec()
