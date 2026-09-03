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
import time
from datetime import datetime
import json
import hashlib
from typing import Iterable, Any
from psycopg.types.json import Jsonb

from em_health.db_client import PgClient
from em_health.utils.tools import logger, profile

TEM_SCHEMA_VERSION = 7
SEM_SCHEMA_VERSION = 7


class DatabaseManager(PgClient):
    """ Manager class to operate on existing db.
    Example usage:
        with DatabaseManager(dbname) as db:
            ...
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.instrument_name = None

    @staticmethod
    def compute_enum_hash(enums: dict) -> bytes:
        """ Compute 16 bytes hash value for given dict of enumerations. """
        data = json.dumps(
            enums,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")

        return hashlib.md5(data).digest()

    def add_instrument(self,
                       instr_dict: dict,
                       config_dict: dict) -> int:
        """ Populate the instrument metadata tables.
        :param instr_dict: input dict with microscope metadata
        :param config_dict: input params+enums dict
        :return: instrument id
        """
        self.instrument_name = instr_dict["name"]
        config_hash = self.compute_enum_hash(config_dict)

        instrument_id = self.run_query(
            "SELECT * FROM events.import_instrument(%s, %s, %s)",
            values=(Jsonb(instr_dict), config_hash, Jsonb(config_dict)),
            mode="fetchone")[0]

        logger.info("Instrument imported", extra={"prefix": self.instrument_name})

        return instrument_id

    #@profile
    def write_data(self,
                   rows: Iterable[tuple],
                   chunk_size: int = 8*1024*1024) -> None:
        """ Write raw values to the data_staging table using COPY and a pre-serialized text buffer,
         then move to the data table. We have to go through staging table because COPY doesn't support ON CONFLICT.
        :param rows: Iterable of tuples
        :param chunk_size: Number of bytes to read at a time
        """
        query = """
            COPY events.data_staging (time, instrument_id, param_id, value_num, value_text)
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

        query = """
            INSERT INTO events.data(time, instrument_id, param_id, value_num, value_text)
            SELECT time, instrument_id, param_id, value_num, value_text
            FROM events.data_staging
            ON CONFLICT DO NOTHING;
            TRUNCATE TABLE events.data_staging;
        """
        self.cur.execute(query)
        self.conn.commit()
        t1 = time.perf_counter()
        logger.debug(f"INSERT into events.data done in: {t1-t0:.4f} s")

        logger.info("Updated events.data table (%d rows)", self.cur.rowcount,
                    extra={"prefix": self.instrument_name})

    def drop_mview(self, view: str, is_cagg: bool = False) -> None:
        """ Delete a materialized view. """
        self.run_query("DROP MATERIALIZED VIEW IF EXISTS {view} CASCADE",
                       {"view": view})
        if not is_cagg:
            # for standard mat. views we need to manually remove the job
            proc = f"refresh_{view.split('.')[-1]}"
            self.run_query("""
                SELECT delete_job(job_id)
                FROM timescaledb_information.jobs
                WHERE proc_name = {proc}
            """, strings={"proc": proc})

            self.run_query("DROP PROCEDURE IF EXISTS {view}",
                           {"view": view})

        logger.info("Dropped MVIEW %s", view)

    def schedule_mview_refresh(self, view: str, interval: str = "12 hours") -> None:
        """ Schedule a materialized view refresh. """
        proc = f"refresh_{view.split('.')[-1]}"

        self.run_query("""
            CREATE OR REPLACE PROCEDURE {proc}(
                job_id int,
                config jsonb
            )
            LANGUAGE SQL
            AS $$
              REFRESH MATERIALIZED VIEW {view};
            $$;
        """, {"proc": proc, "view": view})

        self.run_query("SELECT add_job({proc}, {period})",
                       strings={"proc": proc, "period": interval})
        logger.info("Scheduled MVIEW refresh for %s every %s", view, interval)

    def schedule_cagg_refresh(self,
                              view: str,
                              start_offset: str = "4 days",
                              end_offset: str = "1 day",
                              interval: str = "12 hours") -> None:
        """ Schedule a cont. aggregate refresh.
        :param view:
        :param start_offset: data before start offset will not be recalculated
        :param end_offset: data after end offset is not included
        :param interval: refresh interval
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
        self.run_query("""
            SELECT add_continuous_aggregate_policy({view},
            start_offset => INTERVAL {start_offset},
            end_offset => INTERVAL {end_offset},
            schedule_interval => INTERVAL {schedule_interval})
        """, strings={
            "view": view,
            "start_offset": start_offset,
            "end_offset": end_offset,
            "schedule_interval": interval
        })
        logger.info("Scheduled CAGG refresh for %s every %s", view, interval)

    def force_refresh_cagg(self, view: str) -> None:
        """ Force a cont. aggregate refresh.
        The WITH NO DATA option allows the continuous aggregate to be created
        instantly, so you don't have to wait for the data to be aggregated.
        Here we aggregate all historical data that has been imported so far.
        """
        self.conn.autocommit = True  # required since CALL cannot be executed inside a transaction
        self.run_query("CALL refresh_continuous_aggregate({view}, NULL, NULL)",
                       strings={"view": view})
        self.conn.autocommit = False
        logger.info("Forced CAGG refresh for %s", view)

    def enable_rt_cagg(self, view: str) -> None:
        """ Real-time aggregates automatically add the most recent data when
        you query your continuous aggregate. """
        self.run_query("ALTER MATERIALIZED VIEW {view} set (timescaledb.materialized_only = false)",
                       {"view": view})

    def create_mview(self, view: str, target: str) -> None:
        """ Create a new materialized view or a continuous aggregate. """
        view_fn = self.get_path(target)
        self.execute_file(view_fn)
        logger.info("Created MVIEW %s", view)

    def migrate_db(self, latest_ver: int):
        """ Migrate db to the latest version. """
        current_ver = self.run_query("SELECT version FROM public.schema_info", mode="fetchone")
        current_ver = current_ver[0]
        logger.info("Current schema version: %s", current_ver)

        if current_ver < latest_ver:
            for v in range(current_ver + 1, latest_ver + 1):
                view_fn = self.get_path(target=f"migrations/{v:03d}.sql")
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
            SELECT id, server FROM events.instruments
            WHERE server IS NOT NULL
        """, mode="fetchall")

        if not servers:
            raise ValueError("No servers found in the events.instruments table")

        from em_health.fdw_manager import FDWManager

        for instr_id, server in servers:
            fdw = FDWManager(self, "tds_fdw", str(server), instr_id)
            job_name = fdw.setup_import_job_ms()
            self.run_query("SELECT add_job({jobname}, schedule_interval=>'1 hour')",
                           strings={"jobname": job_name})

            logger.info("Scheduled UEC import job for instrument %s", instr_id)

    def prune_data(self, days: int) -> None:
        """ Prune old data from a database. """
        chunks_dropped = self.run_query("SELECT events.purge_old_chunks('events.data', %s)",
                                        values=(days,), mode='fetchone')
        self.run_query("DELETE FROM uec.errors WHERE time < (current_timestamp - INTERVAL '{days} days')",
                       strings={'days': days})

        self.conn.commit()
        logger.debug("Removed %s chunks from events.data; %s rows from uec.errors", chunks_dropped[0], self.cur.rowcount)


def main(dbname, action, days=None):
    if action == "create-stats":
        logger.info("Running aggregation on database %s", dbname)
        mviews = {
            "tem": {
                # name: is_cagg
                "events.em_off_daily": True,
                "events.em_off": False, # depends on em_off_daily
                "events.epu_sessions": False,
                "events.tomo_sessions": False,
                "events.load_counters_daily": True,
                "events.data_counters_daily": True,
                "events.image_counters_daily": True,
                "events.epu_state_daily": True,
                "events.tomo_state_daily": True,
                "events.tem_cryocycle_daily": True,
                "events.tem_pressure_hourly": True,
                "events.tem_pressure_daily": True, # depends on tem_pressure_hourly
                #"events.tem_feg_counter_daily": True,

                # Depends on em_off
                "events.vacuum_state_daily": False,

                # Depend on *_sessions
                "events.epu_runs": False,
                "events.epud_runs": False,
                "events.tomo_runs": False,
                "events.epu_counters": False,
                "events.tomo_counters": False,

                # Depend on *_state_daily
                "events.epu_running_daily": False,
                "events.tomo_running_daily": False,
            },
            "sem": {
                "events.em_off_daily": True,
                "events.em_off": False, # depends on em_off_daily
                "events.load_counters_daily": True,
                "events.fib_baa_counters_daily": True,
                "events.fib_bda_counters_daily": True,
                "events.gis_counters_daily": True,
                "events.fib_beam_daily": True,
                "events.sem_beam_daily": True,
                "events.flm_beam_daily": True,
                "events.sem_cryocycle_al_daily": True,
                "events.sem_cryocycle_noal_daily": True,
                "events.sem_chamber_state_daily": True,
                "events.sem_pressure_hourly": True,
                "events.sem_pressure_daily": True, # depends on sem_pressure_hourly
                #"events.sem_source_counter_daily": True,
                #"events.fibsem_apertures_daily": True,

                # Depends on the views above
                "events.sem_beamtime_daily": False,
            }
        }

        with DatabaseManager(dbname) as db:
            for view, is_cagg in mviews[dbname].items():
                schema, name = view.split(".")
                db.drop_mview(view)
                if is_cagg:
                    db.create_mview(view, f"{schema}/cagg/{name}.sql")
                    db.force_refresh_cagg(view)
                    if name.endswith("hourly"):
                        db.schedule_cagg_refresh(view, start_offset="6 hours", end_offset="1 hour", interval="1 hour")
                    elif name.endswith("daily"):
                        db.schedule_cagg_refresh(view, start_offset="4 days",  end_offset="1 day", interval="12 hours")
                    db.enable_rt_cagg(view)
                else:
                    db.create_mview(view, f"{schema}/mviews/{name}.sql")
                    db.schedule_mview_refresh(view)

    elif action == "erase":
        print(f"!!! WARNING: You are about to DELETE ALL DATA from database {dbname} !!!")
        confirm = input("Type YES to continue: ")
        if confirm != "YES":
            print("Aborted.")
            return
        logger.info("Deleting ALL data from database %s", dbname)

        from em_health.utils.maintenance import erase_db
        erase_db(dbname, do_init=True)

    elif action == "prune":
        with DatabaseManager(dbname) as db:
            logger.info("Deleting data older than %d days from database %s", days, dbname)
            db.prune_data(days)

    elif action == "migrate":
        latest_ver = TEM_SCHEMA_VERSION if dbname == "tem" else SEM_SCHEMA_VERSION
        if latest_ver is not None:
            # requires superuser permissions
            with DatabaseManager(dbname, username="postgres", password="POSTGRES_PASSWORD") as db:
                db.migrate_db(int(latest_ver))
        else:
            raise ValueError("Could not get latest schema version")

    elif action == "import-uec":
        # requires superuser permissions to create FDW
        with DatabaseManager(dbname, username="postgres", password="POSTGRES_PASSWORD") as db:
            db.import_uec()
