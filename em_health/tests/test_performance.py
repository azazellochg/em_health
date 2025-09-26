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
import csv
import json
import gzip
import random
import time
import statistics
from pathlib import Path
from typing import Iterable
import numpy as np
from datetime import datetime, timedelta

from em_health.db_manager import DatabaseManager
from em_health.utils.maintenance import run_command
from em_health.utils.logs import logger


class TestPerformance:
    def __init__(self, action: str):
        self.action = action

    def run(self):
        if self.action == "gen-data":
            self.simulate_data(target_rows=50_000_000)
        elif self.action == "bench-copy":
            self.test_copy(copy_chunk_size=8*1024*1024, trials=5)
        elif self.action == "bench-write":
            self.test_import(copy_chunk_size=8*1024*1024, nocopy=False, trials=1,
                             table_chunk_size="3 days", table_compression="7 days")
        elif self.action == "bench-query":
            self.test_query()

    @staticmethod
    def create_test_db():
        """ Helper method to create a test database. """
        cmd = r"""
            docker exec timescaledb bash -c "\
            psql -d postgres -c \"DROP DATABASE IF EXISTS benchmark;\" && \
            psql -d postgres -c \"CREATE DATABASE benchmark;\" && \
            psql -d benchmark -c \"CREATE EXTENSION timescaledb CASCADE;\" && \
            psql -d benchmark -c \"CREATE EXTENSION timescaledb_toolkit CASCADE;\" && \
            psql -d benchmark -c \"CREATE EXTENSION pg_stat_statements;\" && \
            psql -d benchmark -c \"CREATE EXTENSION pgstattuple;\" && \
            psql -d benchmark -c \"CREATE EXTENSION pgtap;\" && \
            psql -d benchmark -c \"CREATE EXTENSION tds_fdw;\" && \
            psql -d benchmark -c \"CREATE EXTENSION postgres_fdw;\""
        """
        run_command(cmd)

    @staticmethod
    def stream_file_chunks(file_path: str, max_size: int) -> Iterable[str]:
        """
        Yield chunks of text from a CSV (or gzipped CSV) file.
        Each chunk is ~max_size bytes.
        """
        buffer: list[str] = []
        size = 0

        # Detect gzipped file by extension
        open_func = gzip.open if file_path.endswith(".gz") else open
        with open_func(file_path, "rt", encoding="utf-8") as f:
            for line in f:
                buffer.append(line)
                size += len(line.encode("utf-8"))  # measure bytes
                if size >= max_size:
                    yield "".join(buffer)
                    buffer.clear()
                    size = 0
            if buffer:
                yield "".join(buffer)

    @staticmethod
    def simulate_data(
            filename: str = "simulated_data.csv",
            days: int = 30,
            n_instruments: int = 10,
            min_params: int = 500,
            max_params: int = 1500,
            target_rows: int = 50_000_000,
            chunk_size: int = 1_000_000,
    ):
        """ Generate CSV file with datapoints using NumPy for speed.
        Rows are grouped by param_id and sorted by time within each group to match real XML data.
        """
        t0 = time.perf_counter()
        start_time = datetime(2023, 1, 1, 0, 0, 0)
        end_time = start_time + timedelta(days=days)
        max_timestamp = end_time.timestamp()

        # assign param counts per instrument
        params_per_instrument = {
            inst_id: random.randint(min_params, max_params)
            for inst_id in range(1, n_instruments + 1)
        }

        # assign a frequency class to each param
        param_classes = []
        for inst_id, n_params in params_per_instrument.items():
            for param_id in range(1, n_params + 1):
                freq_class = random.choices(
                    ["high", "medium", "low"], weights=[0.1, 0.3, 0.6], k=1
                )[0]
                param_classes.append((inst_id, param_id, freq_class))

        # assign row counts proportionally to frequency class
        weights = {"high": 100, "medium": 10, "low": 1}
        total_weight = sum(weights[f] for _, _, f in param_classes)
        rows_per_weight = target_rows / total_weight

        with open(filename, "w", newline="") as f:
            writer = csv.writer(f)

            for inst_id, param_id, freq_class in param_classes:
                n_rows = int(weights[freq_class] * rows_per_weight)
                if n_rows == 0:
                    continue

                # vectorized time steps
                if freq_class == "high":
                    steps = np.random.randint(1, 31, size=n_rows)  # seconds
                    deltas = np.cumsum(steps)
                    timestamps = start_time.timestamp() + deltas
                elif freq_class == "medium":
                    steps = np.random.randint(1, 11, size=n_rows) * 60  # minutes
                    deltas = np.cumsum(steps)
                    timestamps = start_time.timestamp() + deltas
                else:  # low
                    steps = np.full(n_rows, 24 * 3600)  # 1 day
                    deltas = np.cumsum(steps)
                    timestamps = start_time.timestamp() + deltas

                # filter within end_time
                mask = timestamps <= max_timestamp
                timestamps = timestamps[mask]

                # generate values: 30% int, 70% float
                n = len(timestamps)
                is_int = np.random.rand(n) < 0.3
                values = np.where(
                    is_int,
                    np.random.randint(0, 1001, size=n),
                    np.round(np.random.uniform(0, 1000, size=n), 3),
                )

                # convert timestamps to formatted strings
                dt_array = np.vectorize(
                    lambda ts: datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                )(timestamps)

                # batch write to CSV
                for i in range(0, n, chunk_size):
                    # time, instr_id, param_id, value_num, value_text
                    rows = zip(
                        dt_array[i:i + chunk_size],
                        np.full(min(chunk_size, n - i), inst_id),
                        np.full(min(chunk_size, n - i), param_id),
                        values[i:i + chunk_size],
                        [""] * min(chunk_size, n - i),
                    )
                    writer.writerows(rows)

        t = time.perf_counter() - t0
        logger.debug(f"Finished generating data (~{target_rows:,} rows) into {filename}")
        logger.debug(f"\tTime: {t:.4f} s")

    def test_copy(
            self,
            filename: str= "simulated_data.csv",
            copy_chunk_size: int = 8 * 1024 * 1024,
            trials: int = 5
    ):
        """ Create a test db and use COPY to ingest data into staging table.
        For COPY 50 mln rows is a good estimate for 1 month of data.
        This test can be used to optimize chunk size or global db configuration.
        """
        if not os.path.exists(filename):
            logger.error("Run 'emhealth test gen-data' first!")
            raise FileNotFoundError(filename)

        self.create_test_db()

        with DatabaseManager(db_name="benchmark", username="postgres", password="postgres") as dbm:
            dbm.run_query("""
                CREATE TABLE IF NOT EXISTS public.data_staging (
                    time TIMESTAMPTZ NOT NULL,
                    instrument_id INTEGER NOT NULL,
                    param_id INTEGER NOT NULL,
                    value_num DOUBLE PRECISION,
                    value_text TEXT
                );
            """)

            copy_query = r"""
                COPY public.data_staging (time, instrument_id, param_id, value_num, value_text)
                FROM STDIN WITH CSV NULL ''
            """

            times, tps = [], []
            for _ in range(trials):
                t0 = time.perf_counter()
                with dbm.cur.copy(copy_query) as copy:
                    for chunk in self.stream_file_chunks(filename, max_size=copy_chunk_size):
                        copy.write(chunk)

                elapsed = time.perf_counter() - t0
                rows_inserted = dbm.cur.rowcount
                run_tps = rows_inserted / elapsed
                times.append(elapsed)
                tps.append(run_tps)

        avg_time = statistics.mean(times)
        avg_tps = statistics.mean(tps)

        logger.debug(
            f"Using COPY to ingest {rows_inserted:,} rows into data_staging table:\n"
            f"\tCOPY chunk size: {copy_chunk_size / 1024 / 1024} MB\n"
            f"\tRaw run times: {times}, rows/s: {tps}\n"
            f"\tAvg time over {trials} runs: {avg_time:.4f} s\n"
            f"\tAvg performance: {avg_tps:,.4f} rows/s"
        )

    def test_import(
            self,
            filename: str= "test_data.xml.gz",
            copy_chunk_size: int = 8 * 1024 * 1024,
            nocopy: bool = False,
            table_chunk_size: str = "3 days",
            table_compression: str = "7 days",
            trials: int = 5
    ):
        """ Create a test db and run the whole import pipeline on XML data. """
        if not os.path.exists(filename):
            raise FileNotFoundError(filename)

        self.create_test_db()
        with DatabaseManager(db_name="benchmark", username="postgres", password="postgres") as dbm:
            logger.debug("Creating public tables in the benchmark db")
            dbm.execute_file(dbm.get_path("create_tables.sql", folder="public"),
                             {
                                 "var_data_chunk_size": table_chunk_size,
                                 "var_data_compression": table_compression
                             })

        from em_health.utils.import_xml import ImportXML
        json_fn = (Path(__file__).parents[1] / "instruments.json").resolve()
        with open(json_fn, encoding="utf-8") as f:
            json_info = json.load(f)

        times, tps = [], []
        for _ in range(trials):
            t0 = time.perf_counter()

            parser = ImportXML(filename, json_info)
            parser.parse_enumerations()
            parser.parse_parameters()
            instr_dict = parser.get_microscope_dict()

            with DatabaseManager(db_name="benchmark", username="postgres", password="postgres") as dbm:
                instrument_id = dbm.add_instrument(instr_dict)
                enum_ids = dbm.add_enumerations(instrument_id, parser.enum_values)
                dbm.add_parameters(instrument_id, parser.params, enum_ids)
                datapoints = parser.parse_values(instrument_id, parser.params)
                dbm.write_data(datapoints, nocopy=nocopy, chunk_size=copy_chunk_size)
                elapsed = time.perf_counter() - t0

                rows_inserted = dbm.cur.rowcount
                run_tps = rows_inserted / elapsed
                times.append(elapsed)
                tps.append(run_tps)

        avg_time = statistics.mean(times)
        avg_tps = statistics.mean(tps)

        logger.debug(
            f"Using {"EXECUTEMANY" if nocopy else "COPY"} to ingest XML data:\n"
            f"\tCOPY chunk size: {copy_chunk_size / 1024 / 1024} MB\n"
            f"\tHypertable chunk size: {table_chunk_size}\n"
            f"\tHypertable compression: {table_compression}\n"
            f"\tRaw run times: {times}, rows/s: {tps}\n"
            f"\tAvg time over {trials} runs: {avg_time:.4f} s\n"
            f"\tAvg performance: {avg_tps:,.4f} rows/s"
        )

    def test_query(self):
        """ Test common queries execution. """
        pass
