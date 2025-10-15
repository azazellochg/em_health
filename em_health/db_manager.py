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
import time
from datetime import datetime, timezone
from typing import Iterable, Optional, Any

from em_health.db_client import PgClient
from em_health.utils.tools import logger, profile


class DatabaseManager(PgClient):
    """ Manager class to operate on existing db.
    Example usage:
        with DatabaseManager(dbname) as db:
            ...
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.instrument_name = None

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

    def add_instrument(self, instr_dict: dict) -> int:
        """ Populate the instrument metadata table.
        :param instr_dict: input dict with microscope metadata
        :return: id and of the new or existing instrument
        """
        self.instrument_name = instr_dict["name"]
        instrument_id = self.run_query("""
            INSERT INTO instruments (instrument, serial, model, name, template, server)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (instrument) DO UPDATE SET instrument = EXCLUDED.instrument
            RETURNING id;
        """, values=(
            instr_dict["instrument"],
            instr_dict["serial"],
            instr_dict["model"],
            self.instrument_name,
            instr_dict["template"],
            instr_dict["server"]
        ), mode="fetchone")[0]

        logger.info("Updated public.instruments table", extra={"prefix": self.instrument_name})

        return instrument_id

    def add_enumerations(self,
                         instrument_id: int,
                         enums_dict: dict) -> dict[str, int]:
        """ Populate the enumerations for TEM or SEM.
        Each enum value is stored as a separate SQL row.
        :param instrument_id: Instrument id
        :param enums_dict: input dict
        :return a dict {enum_types.name: enum_types.id}
        """
        # Batch insert enum_types
        self.cur.executemany("""
            INSERT INTO public.enum_types (instrument_id, name)
            VALUES (%s, %s)
            ON CONFLICT DO NOTHING
        """, (
            (instrument_id, enum_name)
            for enum_name in enums_dict.keys()
        ))
        logger.info("Updated public.enum_types table (%d rows)", self.cur.rowcount,
                    extra={"prefix": self.instrument_name})

        # Fetch IDs for ALL enums
        rows = self.run_query("SELECT id, name FROM public.enum_types WHERE instrument_id = %s",
                              values=(instrument_id,),
                              mode="fetchall")
        enum_name_to_id = {name: eid for eid, name in rows}

        # Batch insert enum_values
        self.cur.executemany("""
            INSERT INTO public.enum_values (enum_id, member_name, value)
            VALUES (%s, %s, %s)
        """, (
            (enum_name_to_id[enum_name], member_name, value)
            for enum_name, data in enums_dict.items()
            for member_name, value in data.items()
        ))

        logger.info("Updated public.enum_values table (%d rows)", self.cur.rowcount,
                    extra={"prefix": self.instrument_name})

        self.conn.commit()

        return enum_name_to_id

    def add_parameters(self,
                       instrument_id: int,
                       params_dict: dict,
                       enums_ids: dict) -> None:
        """ Populate parameters table with associated metadata.
        :param instrument_id: Instrument id
        :param params_dict: input params dict
        :param enums_ids: input enums dict
        """
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
        self.conn.commit()
        logger.info("Updated public.parameters table (%d rows)", self.cur.rowcount,
                    extra={"prefix": self.instrument_name})

    #@profile
    def write_data(self,
                   rows: Iterable[tuple],
                   nocopy: bool = False,
                   chunk_size: int = 8*1024*1024) -> None:
        """ Write raw values to the data table using COPY and a pre-serialized text buffer.
        We do not sort input data, since:
         - for each parameter XML file has a batch of datapoints already sorted by time
         - TimescaleDB data table has chunking with compression, chunks will be sorted by time

        :param rows: Iterable of tuples
        :param nocopy: If True, revert to executemany
        :param chunk_size: Number of bytes to read at a time
        """
        if nocopy:
            query = """
                INSERT INTO public.data (time, instrument_id, param_id, value_num, value_text)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """
            self.cur.executemany(query, rows)
            self.conn.commit()

        else:
            # staging table can have duplicate rows, they will be omitted later
            query = """
                COPY public.data_staging (time, instrument_id, param_id, value_num, value_text)
                FROM STDIN WITH (FORMAT text)
            """

            def format_col(col: Any) -> str:
                if col is None:
                    return "\\N"
                if isinstance(col, datetime):
                    # PostgreSQL COPY expects 'YYYY-MM-DD HH:MM:SS.sss+00' ISO 8601 string
                    return col.strftime("%Y-%m-%d %H:%M:%S.%f%z")[:-3]
                return str(col)

            def stream_chunks(rows: Iterable[tuple], max_size: int) -> Iterable[str]:
                buffer: list[str] = []
                size = 0
                for row in rows:
                    newrow = "\t".join(format_col(col) for col in row) + "\n"
                    encoded = newrow.encode("utf-8")
                    buffer.append(newrow)
                    size += len(encoded)
                    if size >= max_size:
                        yield ''.join(buffer)
                        buffer.clear()
                        size = 0
                if buffer:
                    yield ''.join(buffer)

            # avg row size is ~ 48 bytes, below will give about ~175k rows per chunk
            max_size = int(os.getenv("WRITE_DATA_CHUNK_SIZE", chunk_size))  # 8 Mb
            t0 = time.perf_counter()
            with self.cur.copy(query) as copy:
                for chunk in stream_chunks(rows, max_size):
                    copy.write(chunk)
            t1 = time.perf_counter()
            logger.debug(f"COPY to public.data_staging done in: {t1-t0:.4f} s")

            # order by time before inserting to minimize Timescale switches between chunks
            query = """
                        INSERT INTO public.data(time, instrument_id, param_id, value_num, value_text)
                        SELECT time, instrument_id, param_id, value_num, value_text
                        FROM data_staging
                        ORDER BY time
                        ON CONFLICT DO NOTHING;
                        TRUNCATE TABLE data_staging;
                    """
            self.cur.execute(query)
            self.conn.commit()
            t2 = time.perf_counter()
            logger.debug(f"INSERT into public.data done in: {t2-t1:.4f} s")

        logger.info("Updated public.data table (%d rows)", self.cur.rowcount,
                    extra={"prefix": self.instrument_name})

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
        period = os.getenv("CAGG_REFRESH_INTERVAL", "12 hours")

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

        view_fn = self.get_path(target=name+".sql", folder=schema)
        self.execute_file(view_fn)
        logger.info("Created materialized view %s.%s", schema, name)

    def migrate_db(self, latest_ver: int):
        """ Migrate db to the latest version. """
        current_ver = self.run_query("SELECT version FROM public.schema_info", mode="fetchone")
        current_ver = current_ver[0]
        logger.info("Current schema version: %s", current_ver)

        if current_ver < latest_ver:
            for v in range(current_ver + 1, latest_ver + 1):
                view_fn = self.get_path(target=f"{v:03d}.sql", folder="migrations")
                self.execute_file(view_fn)
            logger.info("Database schema migrated to version %s", latest_ver)
        elif current_ver == latest_ver:
            logger.info("Database schema is up-to-date")
        else:
            raise ValueError("Schema version is higher than expected")

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

        from em_health.fdw_manager import FDWManager

        for instr_id, server in servers:
            fdw = FDWManager(self, "tds_fdw", str(server), instr_id)
            job_name = fdw.setup_import_job_ms()
            self.run_query("SELECT add_job({jobname}, schedule_interval=>'1 hour')",
                           strings={"jobname": job_name})

            logger.info("Scheduled UEC import job for instrument %s", instr_id)


def main(dbname, action, instrument=None, date=None):
    if action == "create-stats":
        logger.info("Running aggregation on database %s", dbname)
        mviews: dict[str, bool] = {
            # name: is_cagg
            "tem_off": False,
            "vacuum_state_daily": False,

            "epu_sessions": False,
            "tomo_sessions": False,

            "epu_runs": False,
            "tomo_runs": False,

            "epu_state_daily": True,
            "tomo_state_daily": True,

            "epu_running_daily": False,
            "tomo_running_daily": False,

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

    elif action == "clean-all":
        print(f"!!! WARNING: You are about to DELETE ALL DATA from database {dbname} !!!")
        confirm = input("Type YES to continue: ")
        if confirm != "YES":
            print("Aborted.")
            return
        logger.info("Deleting ALL data from database %s", dbname)

        from em_health.utils.maintenance import erase_db
        erase_db(dbname, do_init=True)

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
