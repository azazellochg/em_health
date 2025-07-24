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
import psycopg
from psycopg import sql
from pathlib import Path
from typing import Literal, Optional, Dict, Any

from em_health.utils.logs import logger


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
            self.conn = psycopg.connect(
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
        """ Rollback changes and exit on error. """
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

    def execute_file(self, fn) -> None:
        """ Execute an SQL file.
        :param fn: Path to the .sql file.
        """
        if not os.path.exists(fn):
            raise FileNotFoundError(fn)
        with open(fn) as f:
            raw_sql = f.read()

        self.cur.execute(raw_sql)
        self.conn.commit()

    def clean_db(self) -> None:
        """ Erase all public tables in the database. """
        tables = self.run_query("""
            SELECT tablename
            FROM pg_tables
            WHERE schemaname = 'public';
        """, mode="fetchall")

        for table in tables:
            table_name = table[0]
            self.run_query("DROP TABLE IF EXISTS public.{table} CASCADE",
                           {"table": table_name})
            logger.info("Dropped table: %s", table_name)

        self.conn.commit()

    def create_tables(self) -> None:
        """ Create tables in the database. """
        fn = self.get_path("init-tables.sql", folder="../docker")
        self.execute_file(fn)
        logger.info("Created public tables")

    def run_query(
            self,
            query: str,
            identifiers: Optional[Dict[str, str]] = None,
            strings: Optional[Dict[str, Any]] = None,
            values: Optional[tuple] = None,
            mode: Literal["fetchone", "fetchmany", "fetchall", "commit", None] = "commit",
            row_factory: Optional[Any] = None,
    ):
        """
        Execute an SQL query and optionally return results.

        :param query: SQL query string with placeholders for identifiers and literals.
        :param identifiers: dict for table/column identifiers, safely quoted.
        :param strings: dict for literal values to be embedded (strings, etc.).
        :param values: tuple for parameterized query values (%s placeholders).
        :param mode: fetch mode or commit.
        :param row_factory: cursor row factory to customize row output.
        """
        if row_factory is not None:
            self.cur.row_factory = row_factory

        # Compose SQL query with identifiers and literals
        sql_query = sql.SQL(query)
        format_args = {}

        if identifiers:
            format_args.update({k: sql.Identifier(v) for k, v in identifiers.items()})
        if strings:
            format_args.update({k: sql.Literal(v) for k, v in strings.items()})

        sql_query = sql_query.format(**format_args)
        logger.debug("Executing query:\n%s", sql_query.as_string(self.conn))

        self.cur.execute(sql_query, values)

        if mode == "fetchone":
            return self.cur.fetchone()
        if mode == "fetchmany":
            return self.cur.fetchmany()
        if mode == "fetchall":
            return self.cur.fetchall()
        if mode == "commit":
            self.conn.commit()
        # else None implicitly returned

    @staticmethod
    def get_path(target: str, folder: Optional[str] = None) -> Path:
        """ Build a full path starting from the current file's directory.
        :param target: Target file name.
        :param folder: Optional subfolder name.
        :return: Absolute Path object.
        """
        base_dir = Path(__file__).parent
        if folder:
            return (base_dir / folder / target).resolve()
        return (base_dir / target).resolve()
