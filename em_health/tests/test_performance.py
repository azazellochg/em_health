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
from typing import Iterable, Callable, Tuple
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

from em_health.db_manager import DatabaseManager
from em_health.utils.tools import logger, run_command


DB_NAME = "benchmark"
DB_USER = "postgres"
DB_PASS = "postgres"
DEFAULT_FILENAME = "simulated_data.csv"


# ---------------------------------------------------------------------------
# Utility classes
# ---------------------------------------------------------------------------
class PerfStats:
    """Helper for collecting and summarizing timing/performance metrics."""

    def __init__(self) -> None:
        self.times: list[float] = []
        self.throughputs: list[float] = []

    def record(self, rows: int, elapsed: float) -> None:
        self.times.append(elapsed)
        self.throughputs.append(rows / elapsed)

    def summary(self, trials: int) -> dict[str, float]:
        return {
            "avg_time": statistics.mean(self.times) if self.times else 0.0,
            "avg_tps": statistics.mean(self.throughputs) if self.throughputs else 0.0,
            "trials": trials
        }


class CSVLoader:
    """CSV and gzipped CSV utilities."""

    @staticmethod
    def load(filename: str):
        df = pd.read_csv(
            filename,
            header=None,
            names=["time", "instrument_id", "param_id", "value_num", "value_text"],
            dtype={
                "instrument_id": "int32",
                "param_id": "int32",
                "value_num": "float64",
                "value_text": "string",
            },
        )
        # parse time column
        df["time"] = pd.to_datetime(df["time"], format="%Y-%m-%d %H:%M:%S.%f")

        # replace empty strings with None
        df["value_text"] = df["value_text"].replace("", pd.NA)

        # convert to list of tuples for DB insertion
        return list(df.itertuples(index=False, name=None))

    @staticmethod
    def stream_chunks(file_path: str, max_size: int) -> Iterable[str]:
        """Yield ~max_size byte chunks from a CSV (or gzipped CSV)."""
        buffer: list[str] = []
        size = 0

        open_func = gzip.open if file_path.endswith(".gz") else open
        with open_func(file_path, "rt", encoding="utf-8") as f:
            for line in f:
                buffer.append(line)
                size += len(line.encode("utf-8"))
                if size >= max_size:
                    yield "".join(buffer)
                    buffer.clear()
                    size = 0
            if buffer:
                yield "".join(buffer)


class DataSimulator:
    """Generate synthetic CSV data for benchmarking."""

    def __init__(self, filename: str = DEFAULT_FILENAME):
        self.filename = filename

    def simulate(
        self,
        batch: int,
        days: int = 30,
        n_instruments: int = 10,
        min_params: int = 500,
        max_params: int = 1500,
    ) -> None:
        t0 = time.perf_counter()
        chunk_size = batch // 50
        start_time = datetime(2023, 1, 1, 0, 0, 0)
        end_time = start_time + timedelta(days=days)
        max_timestamp = end_time.timestamp()

        # random number of params per instrument
        params_per_instrument = {
            inst_id: random.randint(min_params, max_params)
            for inst_id in range(1, n_instruments + 1)
        }

        param_classes = []
        for inst_id, n_params in params_per_instrument.items():
            for param_id in range(1, n_params + 1):
                freq_class = random.choices(
                    ["high", "medium", "low"], weights=[0.1, 0.3, 0.6], k=1
                )[0]
                param_classes.append((inst_id, param_id, freq_class))

        # weight distribution
        weights = {"high": 100, "medium": 10, "low": 1}
        total_weight = sum(weights[f] for _, _, f in param_classes)
        rows_per_weight = batch / total_weight

        with open(self.filename, "w", newline="") as f:
            writer = csv.writer(f)

            for inst_id, param_id, freq_class in param_classes:
                n_rows = int(weights[freq_class] * rows_per_weight)
                if n_rows == 0:
                    continue

                steps = self._random_steps(freq_class, n_rows)
                deltas = np.cumsum(steps)
                timestamps = start_time.timestamp() + deltas
                timestamps = timestamps[timestamps <= max_timestamp]

                n = len(timestamps)
                values = self._random_values(n)
                dt_array = [
                    datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                    for ts in timestamps
                ]

                for i in range(0, n, chunk_size):
                    rows = zip(
                        dt_array[i : i + chunk_size],
                        [inst_id] * min(chunk_size, n - i),
                        [param_id] * min(chunk_size, n - i),
                        values[i : i + chunk_size],
                        [""] * min(chunk_size, n - i),
                    )
                    writer.writerows(rows)

        elapsed = time.perf_counter() - t0
        logger.info(f"Finished generating data (~{batch:,} rows) into {self.filename}")
        logger.info(f"\tTime: {elapsed:.4f} s")

    @staticmethod
    def _random_steps(freq_class: str, n_rows: int) -> np.ndarray:
        if freq_class == "high":
            return np.random.randint(1, 31, size=n_rows)
        elif freq_class == "medium":
            return np.random.randint(1, 11, size=n_rows) * 60
        else:
            return np.full(n_rows, 24 * 3600)

    @staticmethod
    def _random_values(n: int) -> np.ndarray:
        is_int = np.random.rand(n) < 0.3
        return np.where(
            is_int,
            np.random.randint(0, 1001, size=n),
            np.round(np.random.uniform(0, 1000, size=n), 3),
        )


# ---------------------------------------------------------------------------
# Benchmark runner
# ---------------------------------------------------------------------------
class Benchmark:
    def __init__(self, batch: int, filename: str = DEFAULT_FILENAME):
        self.batch = batch
        self.filename = filename

    # --- DB setup ---
    @staticmethod
    def create_test_db() -> None:
        cmd = r"""
            docker exec timescaledb bash -c "\
            psql -d postgres -c \"DROP DATABASE IF EXISTS benchmark;\" && \
            psql -d postgres -c \"CREATE DATABASE benchmark;\" && \
            psql -d benchmark -c \"CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;\" && \
            psql -d benchmark -c \"CREATE EXTENSION IF NOT EXISTS timescaledb_toolkit CASCADE;\" && \
            psql -d benchmark -c \"CREATE EXTENSION IF NOT EXISTS pg_stat_statements;\" && \
            psql -d benchmark -c \"CREATE EXTENSION IF NOT EXISTS pgstattuple;\" && \
            psql -d benchmark -c \"CREATE EXTENSION IF NOT EXISTS pgtap;\" && \
            psql -d benchmark -c \"CREATE EXTENSION IF NOT EXISTS tds_fdw;\" && \
            psql -d benchmark -c \"CREATE EXTENSION IF NOT EXISTS postgres_fdw;\""
        """
        run_command(cmd)

    def with_db(self, fn: Callable[[DatabaseManager], None]) -> None:
        with DatabaseManager(db_name=DB_NAME, username=DB_USER, password=DB_PASS) as dbm:
            fn(dbm)

    # --- Core runner ---
    def run_test(self, method: Callable, trials: int = 5) -> None:
        if not os.path.exists(self.filename):
            logger.error("Run 'emhealth db test-data 1000000' first!")
            raise FileNotFoundError(self.filename)

        self.create_test_db()
        stats = PerfStats()

        def setup_table(dbm: DatabaseManager):
            dbm.run_query(
                """
                CREATE TABLE IF NOT EXISTS public.data_staging (
                    time TIMESTAMPTZ NOT NULL,
                    instrument_id INTEGER NOT NULL,
                    param_id INTEGER NOT NULL,
                    value_num DOUBLE PRECISION,
                    value_text TEXT
                );
                """
            )

        self.with_db(setup_table)

        rows_inserted, header = 0, ""
        for _ in range(trials):
            t0 = time.perf_counter()
            rows_inserted, header = self.with_db(lambda dbm: method(dbm)) or (0, "")
            elapsed = time.perf_counter() - t0
            stats.record(rows_inserted, elapsed)

        summary = stats.summary(trials)
        calls, plan_time, exec_time = self.get_stats(header)

        logger.info(
            f"Using {method.__name__} to insert {rows_inserted:,} rows into data_staging table:\n"
            f"\tBatch size: {self.batch}\n"
            f"\tRaw run times: {stats.times}, rows/s: {stats.throughputs}\n"
            f"\tAvg time over {summary['trials']} runs: {summary['avg_time']:.4f} s\n"
            f"\tAvg performance: {summary['avg_tps']:,.4f} rows/s\n"
            f"\tCalls per run: {calls // trials}\n"
            f"\tPlan time per call: {plan_time:.4f} ms\n"
            f"\tExec time per call: {exec_time:.4f} ms"
        )

    @staticmethod
    def get_stats(header: str) -> Tuple[int, float, float]:
        with DatabaseManager(db_name=DB_NAME, username=DB_USER, password=DB_PASS) as dbm:
            r = dbm.run_query(
                f"""
                SELECT calls,
                       total_plan_time/NULLIF(calls, 0) AS plan_time,
                       total_exec_time/NULLIF(calls, 0) AS exec_time
                FROM pg_stat_statements s
                JOIN pg_database d ON s.dbid = d.oid
                WHERE d.datname = '{DB_NAME}'
                  AND s.query LIKE '{header}%'
                """,
                mode="fetchall",
            )
        return r[0] if r else (0, 0, 0)

    # --- Insert methods ---
    def insert_copy(self, dbm: DatabaseManager) -> Tuple[int, str]:
        chunk_size = self.batch
        header = f"--COPY_{chunk_size}"
        query = f"""{header}
            COPY public.data_staging (time, instrument_id, param_id, value_num, value_text)
            FROM STDIN WITH CSV NULL ''
        """
        with dbm.cur.copy(query) as copy:
            for chunk in CSVLoader.stream_chunks(self.filename, max_size=chunk_size):
                copy.write(chunk)
        dbm.conn.commit()
        return dbm.cur.rowcount, header

    def insert_executemany(self, dbm: DatabaseManager) -> Tuple[int, str]:
        header = f"--EXECMANY_{self.batch}"
        query = f"""{header}
            INSERT INTO public.data_staging (time, instrument_id, param_id, value_num, value_text)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
        """
        rows = CSVLoader.load(self.filename)
        for i in range(0, len(rows), self.batch):
            batch = rows[i : i + self.batch]
            dbm.cur.executemany(query, batch)
            dbm.conn.commit()
        return len(rows), header

    def insert_unnest(self, dbm: DatabaseManager) -> Tuple[int, str]:
        header = f"--UNNEST_{self.batch}"
        query = f"""{header}
            INSERT INTO public.data_staging (time, instrument_id, param_id, value_num, value_text)
            SELECT * FROM unnest(%s::timestamp[], %s::int[], %s::int[], %s::float[], %s::text[])
            ON CONFLICT DO NOTHING
        """
        times, inst_ids, param_ids, vals_num, vals_text = zip(*CSVLoader.load(self.filename))
        n = len(times)
        for i in range(0, n, self.batch):
            dbm.cur.execute(
                query,
                (
                    list(times[i : i + self.batch]),
                    list(inst_ids[i : i + self.batch]),
                    list(param_ids[i : i + self.batch]),
                    list(vals_num[i : i + self.batch]),
                    list(vals_text[i : i + self.batch]),
                ),
            )
            dbm.conn.commit()
        return n, header


# ---------------------------------------------------------------------------
# Dispatcher for actions
# ---------------------------------------------------------------------------
class TestPerformance:
    def __init__(self, action: str, batch: int):
        self.action = action
        self.batch = batch
        self.simulator = DataSimulator()
        self.bench = Benchmark(batch)

    def run(self) -> None:
        actions = {
            "test-data": lambda: self.simulator.simulate(self.batch),
            "test-copy": lambda: self.bench.run_test(self.bench.insert_copy),
            "test-execmany": lambda: self.bench.run_test(self.bench.insert_executemany),
            "test-unnest": lambda: self.bench.run_test(self.bench.insert_unnest),
            "test-import": self.test_import,
            "test-query": self.test_query,
        }
        action_fn = actions.get(self.action)
        if not action_fn:
            raise ValueError(f"Unknown action: {self.action}")
        action_fn()

    def test_import(
        self,
        filename: str = "test_data.xml.gz",
        copy_chunk_size: int = 8 * 1024 * 1024,
        nocopy: bool = False,
        table_chunk_size: str = "3 days",
        table_compression: str = "7 days",
        trials: int = 5,
    ) -> None:
        if not os.path.exists(filename):
            raise FileNotFoundError(filename)

        stats = PerfStats()
        for _ in range(trials):
            self.bench.create_test_db()
            with DatabaseManager(db_name=DB_NAME, username=DB_USER, password=DB_PASS) as dbm:
                logger.info("Creating public tables in the benchmark db")
                dbm.execute_file(
                    dbm.get_path("create_tables.sql", folder="public"),
                    {
                        "var_data_chunk_size": table_chunk_size,
                        "var_data_compression": table_compression,
                    }
                )

            from em_health.utils.import_xml import ImportXML

            json_fn = (Path(__file__).parents[1] / "instruments.json").resolve()
            with open(json_fn, encoding="utf-8") as f:
                json_info = json.load(f)

            parser = ImportXML(filename, json_info)
            parser.parse_enumerations()
            parser.parse_parameters()
            instr_dict = parser.get_microscope_dict()

            with DatabaseManager(db_name=DB_NAME, username=DB_USER, password=DB_PASS) as dbm:
                instrument_id = dbm.add_instrument(instr_dict)
                enum_ids = dbm.add_enumerations(instrument_id, parser.enum_values)
                dbm.add_parameters(instrument_id, parser.params, enum_ids)
                datapoints = parser.parse_values(instrument_id, parser.params)

                t0 = time.perf_counter()
                dbm.write_data(datapoints, nocopy=nocopy, chunk_size=copy_chunk_size)
                elapsed = time.perf_counter() - t0

                rows_inserted = dbm.cur.rowcount
                stats.record(rows_inserted, elapsed)

        summary = stats.summary(trials)
        logger.info(
            f"Using {"EXECUTEMANY" if nocopy else "COPY"} to ingest XML data:\n"
            f"\tCOPY chunk size: {copy_chunk_size / 1024 / 1024} MB\n"
            f"\tHypertable chunk size: {table_chunk_size}\n"
            f"\tHypertable compression: {table_compression}\n"
            f"\tRaw run times: {stats.times}, rows/s: {stats.throughputs}\n"
            f"\tAvg time over {summary['trials']} runs: {summary['avg_time']:.4f} s\n"
            f"\tAvg performance: {summary['avg_tps']:,.4f} rows/s\n"
        )

    def test_query(self):
        """ Test common queries execution. """
        pass
