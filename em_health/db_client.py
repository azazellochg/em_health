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
from itertools import islice
import psycopg2
from psycopg2.extras import execute_values
from typing import Literal, Iterable, Optional

from em_health.utils.logs import logger, profile


class DatabaseClient:
    """ Main class that will manage the PostgreSQL client. """
    def __init__(self,
                 db_name: Literal["tem", "sem"],
                 user: Optional[str] = None,
                 password: Optional[str] = None):
        self.db_name = db_name
        self.host = os.getenv('POSTGRES_HOST', 'localhost')
        self.username = user or os.getenv('POSTGRES_USER', 'postgres')
        self.password = password or os.getenv('POSTGRES_PASSWORD', None)
        self.port = 5432
        self.conn = None
        self.cur = None

        if not self.password:
            raise ValueError("POSTGRES_PASSWORD environment variable is not set")

    def __enter__(self):
        """ Establish connection to the database. """
        try:
            self.conn = psycopg2.connect(
                host=self.host,
                port=5432,
                dbname=self.db_name,
                user=self.username,
                password=self.password,
                application_name="HealthMonitor"
            )
            self.cur = self.conn.cursor()
            logger.info("Connected to %s@%s: database %s", self.username, self.host, self.db_name)
            return self
        except Exception as e:
            logger.error("Connection failed: %s", e)
            raise

    def __exit__(self, exc_type, exc_value, traceback):
        """ Exit on error. """
        if self.cur:
            self.cur.close()
        if self.conn:
            if exc_type:
                self.conn.rollback()
                logger.warning("Transaction rolled back due to: %s", exc_value)
            else:
                self.conn.commit()
            self.conn.close()
            logger.info("Connection closed.")

    def close(self):
        """ Close the connection to the database. """
        if self.cur:
            self.cur.close()
        if self.conn:
            self.conn.close()
            logger.info("Connection closed.")

    def clean_db(self):
        """ Erase all tables in the database. """
        self.cur.execute("""
            SELECT tablename
            FROM pg_tables
            WHERE schemaname = 'public';
        """)

        tables = self.cur.fetchall()
        for table in tables:
            table_name = table[0]
            self.cur.execute(f"DROP TABLE IF EXISTS public.{table_name} CASCADE;")
            logger.info("Dropped table: %s", table_name)

        self.conn.commit()

    def add_instrument(self, instr_dict: dict) -> int:
        """ Populate the instrument metadata table.
        :param instr_dict: input dict with microscope metadata
        """
        self.cur.execute("""
            INSERT INTO public.instruments (
                instrument, serial, model, name, template, server
            ) VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (instrument)
            DO UPDATE SET
                serial = EXCLUDED.serial,
                model = EXCLUDED.model,
                name = EXCLUDED.name,
                template = EXCLUDED.template
                server = EXCLUDED.server
            RETURNING id;
        """, (
            str(instr_dict["instrument"]),
            int(instr_dict["serial"]),
            str(instr_dict["model"]),
            str(instr_dict["name"]),
            str(instr_dict["template"]),
            str(instr_dict["server"])
        ))

        self.conn.commit()
        logger.info("Updated instruments table (item %s)", instr_dict["name"])
        instrument_id = self.cur.fetchone()[0]

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
        self.cur.execute("""
            SELECT COALESCE(MAX(enum_id), 0)
            FROM public.enumerations
            WHERE instrument_id = %s;
        """, (instrument_id,))

        max_enum_id = self.cur.fetchone()[0]
        new_enum_id = max_enum_id + 1

        # Prepare insert statement
        insert_sql = """
            INSERT INTO public.enumerations (
            instrument_id, enum_id, enum, name, value
            )
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING;
        """

        # Batch inserts
        for enum, enum_values in enums_dict.items():
            data_to_insert = [
                (instrument_id, new_enum_id, enum, name, value)
                for name, value in enum_values.items()
            ]
            self.cur.executemany(insert_sql, data_to_insert)
            output_dict[enum] = new_enum_id
            new_enum_id += 1

        self.cur.execute("SELECT COUNT(*) FROM public.enumerations;")
        row_count = self.cur.fetchone()[0]
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

        for param_id, p_dict in params_dict.items():
            enum_type = p_dict.get("enum")
            enum_id = enums_dict.get(enum_type) if enum_type else None

            self.cur.execute("""
                INSERT INTO public.parameters (
                    instrument_id,
                    param_id, subsystem, component, param_name, display_name,
                    display_unit, storage_unit, display_scale, enum_id, value_type
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING;
            """, (
                instrument_id,
                param_id,
                p_dict["subsystem"],
                p_dict["component"],
                p_dict["name"],
                p_dict["display_name"],
                p_dict["display_unit"],
                p_dict["storage_unit"],
                p_dict["display_scale"],
                enum_id,
                p_dict["type"]
            ))

        self.cur.execute("SELECT COUNT(*) FROM public.parameters;")
        row_count = self.cur.fetchone()[0]
        self.conn.commit()
        logger.info("Updated parameters table (total %d items)", row_count)

    #@profile
    def write_data(self,
                   rows: Iterable,
                   batch_size: int = 15000) -> None:
        """ Write raw values to the data table.
        We do not sort input data, since:
         - for each parameter XML file has a batch of datapoints already sorted by time
         - TimescaleDB data table has chunking with compression, chunks will be sorted by time
        """
        query = """
           INSERT INTO public.data (
               time, instrument_id, param_id, value_num, value_text
           ) VALUES %s
           ON CONFLICT DO NOTHING;
        """

        def batch_iterator(iterable, batch_size):
            """Yield batches of a specified size from the iterator."""
            while True:
                batch = list(islice(iterable, batch_size))
                if not batch:
                    break
                yield batch

        for batch in batch_iterator(rows, batch_size):
            execute_values(self.cur, query, batch, page_size=1000)
        self.conn.commit()

        self.cur.execute("SELECT COUNT(*) FROM public.data;")
        row_count = self.cur.fetchone()[0]
        logger.info("Updated data table (total %d items)", row_count)
