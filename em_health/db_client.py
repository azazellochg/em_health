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
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Literal, Optional, Dict, Any
import psycopg
from psycopg import sql

from em_health.utils.tools import logger


class BaseDBClient(ABC):
    """Abstract base class for a database client."""
    def __init__(self,
                 db_name: str,
                 default_port: int,
                 **kwargs):
        self.db_name = db_name
        if "username" in kwargs:
            self.username = kwargs["username"]
            self.password = os.getenv(kwargs["password"])
        else:
            self.username = "postgres"
            self.password = os.getenv("POSTGRES_PASSWORD")
        self.port = default_port
        self.conn = None
        self.cur = None

        if not self.password:
            raise ValueError(f"Password is not set")

    def __enter__(self):
        try:
            self.connect()
            return self
        except Exception as e:
            logger.error("Connection failed: %s", e)
            raise

    def __exit__(self, exc_type, exc_value, traceback):
        """ Rollback changes and exit on error. """
        try:
            if exc_type:
                self.conn.rollback()
                logger.warning("Transaction rolled back due to: %s", exc_value)
            else:
                self.conn.commit()
        finally:
            if self.cur:
                self.cur.close()
            if self.conn:
                self.conn.close()
                logger.info("Connection closed.")

    @abstractmethod
    def connect(self):
        ...

    @staticmethod
    def get_path(target: str, folder: Optional[str] = None) -> Path:
        """ Build a full path starting from the em_health/sql directory.
        :param target: Target file name.
        :param folder: Optional subfolder name.
        :return: Absolute Path object.
        """
        base_dir = Path(__file__).parent / "sql"
        if folder:
            return (base_dir / folder / target).resolve()
        return (base_dir / target).resolve()


class PgClient(BaseDBClient):
    """ PostgreSQL DB client. """
    def __init__(self, db_name: str, **kwargs):
        super().__init__(db_name, 5432, **kwargs)
        self.host = os.getenv('POSTGRES_HOST', 'localhost')

    def connect(self):
        self.conn = psycopg.connect(
            host=self.host,
            port=self.port,
            dbname=self.db_name,
            user=self.username,
            password=self.password,
            application_name="EMHealth"
        )
        self.cur = self.conn.cursor()
        logger.info("Connected to PostgreSQL %s@%s: database %s",
                    self.username, self.host, self.db_name)

    def execute_file(self,
                     fn,
                     variables: Optional[dict[str, str]] = None) -> None:
        """ Execute an SQL file.
        :param fn: Path to the .sql file.
        :param variables: Dictionary of variable names and values.
        """
        if not os.path.exists(fn):
            raise FileNotFoundError(fn)
        with open(fn) as f:
            raw_sql = f.read()

        if variables:
            for key, val in variables.items():
                placeholder = f":{key}"
                replacement = f"'{val}'"
                raw_sql = raw_sql.replace(placeholder, replacement)

        self.cur.execute(raw_sql)
        self.conn.commit()

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
        :param row_factory: cursor row factory to customize output.
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
        elif mode == "fetchmany":
            return self.cur.fetchmany()
        elif mode == "fetchall":
            return self.cur.fetchall()
        elif mode == "commit":
            self.conn.commit()
            return None
        else:
            return None
