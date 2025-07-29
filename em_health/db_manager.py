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

import psycopg.errors
from datetime import datetime, timezone
from typing import Iterable, Optional

from em_health.db_client import DatabaseClient
from em_health.utils.logs import logger, profile


class DatabaseManager(DatabaseClient):
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
            # delete all data for instrument
            self.run_query("DELETE FROM public.data WHERE instrument_id = %s",
                           values=(instrument_id,))
            self.run_query("DELETE FROM public.parameters WHERE instrument_id = %s",
                           values=(instrument_id,))
            self.run_query("DELETE FROM public.enumerations WHERE instrument_id = %s",
                           values=(instrument_id,))
            self.run_query("DELETE FROM public.instruments WHERE id = %s",
                           values=(instrument_id,))
            logger.info("Deleted instrument %s", instrument_serial)
        else:
            from_date = datetime.strptime(since, "%d-%m-%Y").replace(tzinfo=timezone.utc)
            self.run_query("DELETE FROM public.data WHERE instrument_id = %s AND time < %s",
                           values=(instrument_id, from_date))
            logger.info("Deleted data older than %s for instrument %s",
                        from_date, instrument_serial)

        self.conn.commit()

    def add_instrument(self, instr_dict: dict) -> int:
        """ Populate the instrument metadata table.
        :param instr_dict: input dict with microscope metadata
        :return: id of a newly created instrument
        """
        row = self.run_query("""
            INSERT INTO public.instruments (
                instrument, serial, model, name, template, server
            ) VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (instrument)
            DO UPDATE SET
                serial = EXCLUDED.serial,
                model = EXCLUDED.model,
                name = EXCLUDED.name,
                template = EXCLUDED.template,
                server = EXCLUDED.server
            RETURNING id
        """, values = (
            instr_dict["instrument"],
            instr_dict["serial"],
            instr_dict["model"],
            instr_dict["name"],
            instr_dict["template"],
            instr_dict["server"]
        ), mode="fetchone")

        logger.info("Updated instruments table (item %s)", instr_dict["name"])
        instrument_id = row[0] if row else None

        return instrument_id

    def add_enumerations(self,
                         instrument_id: int,
                         enums_dict: dict) -> dict:
        """ Populate the enumerations for TEM or SEM.
        Each enum value is stored as a separate SQL row.
        :param instrument_id: Instrument id
        :param enums_dict: input dict
        :return a dict {enum_type: enum_id}
        """
        output_dict = {}
        logger.info("Found %d enumerations (%s values)",
                    len(enums_dict), sum(len(inner) for inner in enums_dict.values()))

        # Get max enum_id
        row = self.run_query("""
            SELECT COALESCE(MAX(enum_id), 0)
            FROM public.enumerations
            WHERE instrument_id = %s
        """, values=(instrument_id,), mode="fetchone")

        max_enum_id = row[0]
        new_enum_id = max_enum_id + 1

        # Prepare insert statement
        insert_sql = """
            INSERT INTO public.enumerations (
            instrument_id, enum_id, enum, name, value
            )
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
        """

        # Batch inserts
        data_to_insert = []
        for enum, enum_values in enums_dict.items():
            data_to_insert.extend(
                (instrument_id, new_enum_id, enum, name, value)
                for name, value in enum_values.items()
            )
            output_dict[enum] = new_enum_id
            new_enum_id += 1

        self.cur.executemany(insert_sql, data_to_insert)

        row = self.run_query("SELECT COUNT(*) FROM public.enumerations", mode="fetchone")
        row_count = row[0] if row else None
        self.conn.commit()
        logger.info("Updated enumerations table (total %d rows)", row_count)

        return output_dict

    def add_parameters(self,
                       instrument_id: int,
                       params_dict: dict,
                       enums_dict: dict) -> None:
        """ Populate parameters table with associated metadata.
        :param instrument_id: Instrument id
        :param params_dict: input params dict
        :param enums_dict: input enums dict
        """
        logger.info("Found %d parameters", len(params_dict))

        insert_sql = """
            INSERT INTO public.parameters (
                instrument_id,
                param_id, subsystem, component, param_name, display_name,
                display_unit, storage_unit, display_scale, enum_id, value_type
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
        """

        # Batch inserts
        data_to_insert = [
            (
                instrument_id,
                param_id,
                p_dict["subsystem"],
                p_dict["component"],
                p_dict["name"],
                p_dict["display_name"],
                p_dict["display_unit"],
                p_dict["storage_unit"],
                p_dict["display_scale"],
                enums_dict.get(enum) if (enum := p_dict.get("enum")) else None,
                p_dict["type"]
            ) for param_id, p_dict in params_dict.items()
        ]

        self.cur.executemany(insert_sql, data_to_insert)

        row = self.run_query("SELECT COUNT(*) FROM public.parameters", mode="fetchone")
        row_count = row[0] if row else None
        self.conn.commit()
        logger.info("Updated parameters table (total %d rows)", row_count)

    #@profile
    def write_data(self,
                   rows: Iterable[tuple],
                   chunk_size: int = 65536,
                   nocopy: bool = False) -> None:
        """ Write raw values to the data table using COPY and a pre-serialized text buffer.
        We do not sort input data, since:
         - for each parameter XML file has a batch of datapoints already sorted by time
         - TimescaleDB data table has chunking with compression, chunks will be sorted by time

        :param rows: Iterable of tuples
        :param chunk_size: Max size in bytes per COPY write
        :param nocopy: If True, revert to executemany with duplicate handling
        """
        if nocopy:
            logger.info("No-copy mode. Duplicate entries are ignored.")
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

            try:
                with self.cur.copy(query) as copy:
                    for chunk in stream_chunks(rows, chunk_size):
                        copy.write(chunk)
            except psycopg.errors.UniqueViolation as e:
                logger.error("Duplicate entries found: %s", e)
                raise

        row = self.run_query("SELECT COUNT(*) FROM public.data", mode="fetchone")
        row_count = row[0] if row else None
        logger.info("Updated data table (total %d rows)", row_count)

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

    def schedule_mview_refresh(self, name: str, period: str = '1d') -> None:
        """ Schedule a materialized view refresh. """
        proc = f"refresh_{name}"

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

    def schedule_cagg_refresh(self, name: str) -> None:
        """ Schedule a cont. aggregate refresh. """
        self.run_query("""
            SELECT add_continuous_aggregate_policy({name},
            start_offset => INTERVAL '7 days',
            end_offset => INTERVAL '6 hours',
            schedule_interval => INTERVAL '12 hours')
        """, strings={"name": name})
        logger.info("Scheduled continuous aggregate refresh for %s", name)

    def force_refresh_cagg(self, name: str) -> None:
        """ Force a cont. aggregate refresh.
        The WITH NO DATA option allows the continuous aggregate to be created
        instantly, so you don't have to wait for the data to be aggregated.
        Data begins to populate only when the policy begins to run. This means
        that only data newer than the start_offset time begins to populate the
        continuous aggregate. If you have historical data that is older than
        the start_offset interval, you need to manually refresh the history
        up to the current start_offset to allow real-time queries to run efficiently.
        """
        self.conn.autocommit = True # required since CALL cannot be executed inside a transaction
        self.run_query("CALL refresh_continuous_aggregate({name}, NULL, localtimestamp - INTERVAL '1 week')",
                       strings={"name": name})
        self.conn.autocommit = False
        logger.info("Forced continuous aggregate refresh for %s", name)

    def create_mview(self, name: str) -> None:
        """ Create a new materialized view or a continuous aggregate. """
        view_fn = self.get_path(target=name+".sql", folder="views")
        self.execute_file(view_fn)
        logger.info("Created materialized view %s", name)


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
                    db.schedule_cagg_refresh(mview)
                    # Refresh only to parse historical data prior to the start_date of the schedule
                    db.force_refresh_cagg(mview)
                else:
                    db.schedule_mview_refresh(mview, '1d')
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
