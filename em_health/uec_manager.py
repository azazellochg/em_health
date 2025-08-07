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
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

from em_health.db_manager import DatabaseManager
from em_health.db_client import MSClient
from em_health.utils.logs import logger


class UECManager:
    """ Manager class to operate on UEC (Alarms) data. """
    def __init__(self, dbname: str):
        self.pgdb_name = dbname
        self.msdb_name = "DS"
        self.servers = None

    def get_servers(self) -> None:
        """ Get a list of servers from the instrument table. """
        if any(os.getenv(var) == "None" for var in ["MSSQL_USER", "MSSQL_PASSWORD"]):
            logger.warning("MSSQL_USER and MSSQL_PASSWORD are not set.")
            sys.exit(0)

        with DatabaseManager(self.pgdb_name) as db:
            self.servers = db.run_query("""
                SELECT
                    id, server, name FROM public.instruments
                WHERE server IS NOT NULL
                """, mode="fetchall")

        if not self.servers:
            raise ValueError("No servers found in the public.instrument table")

    def get_metadata(self, server: str) -> list[tuple]:
        """ Query UEC metadata from MSSQL DB. """
        with MSClient(db_name=self.msdb_name, host=server) as db:
            return db.run_query("""
                SELECT
                    ErrorDefinitionID, SubsystemID, Subsystem, 
                    DeviceTypeID, DeviceType, DeviceInstanceID, 
                    DeviceInstance, ErrorCodeID, ErrorCode
                FROM qry.ErrorDefinitions
                """)

    def get_data(self, server: str) -> list:
        """ Query UEC data from MSSQL DB. """
        query = """
            SELECT 
                ErrorDtm,
                ErrorDefinitionID,
                MessageText
            FROM qry.ErrorNotifications
        """
        with MSClient(db_name=self.msdb_name, host=server) as db:
            return db.run_query(query)

    def import_metadata(self, metadata: list) -> None:
        """ Import UEC metadata into PostgreSQL DB. """
        with DatabaseManager(self.pgdb_name) as db:
            query = """
                    INSERT INTO uec.error_definitions (
                        error_definition_id,
                        subsystem_id,
                        subsystem,
                        device_type_id ,
                        device_type,
                        device_instance_id,
                        device_instance,
                        error_code_id,
                        error_code)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                """
            metadata = [tuple(row) for row in metadata]
            db.cur.executemany(query, metadata)
            db.conn.commit()

    def import_data(self, instrument_id: int, data: list) -> None:
        """ Import UEC data into PostgreSQL DB. """
        with DatabaseManager(self.pgdb_name) as db:
            filtered_data = [
                (row[0], instrument_id, row[1], row[2]) for row in data
            ]

            query = """
                INSERT INTO uec.errors (
                    time,
                    instrument_id,
                    error_id,
                    message_text)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """
            db.cur.executemany(query, filtered_data)
            db.conn.commit()

    def process_server(self,
                       instrument_id: int,
                       server: str, name: str) -> None:
        """ Import UEC data from MSSQL DB into PostgreSQL DB. """
        logger.info("Processing server %s", server,
                    extra={"prefix": name})

        metadata = self.get_metadata(server)
        self.import_metadata(metadata)
        logger.info(f"Imported {len(metadata)} error definitions",
                    extra={"prefix": name})

        data = self.get_data(server)
        if data:
            self.import_data(instrument_id, data)
            logger.info(f"Imported {len(data)} UECs from {server}",
                        extra={"prefix": name})
        else:
            logger.warning("No UECs found on server %s", server,
                           extra={"prefix": name})

    def run_all_tasks(self) -> None:
        """ Run all imports concurrently using a thread pool. """
        self.get_servers()
        workers = min(len(self.servers), 4)

        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [
                executor.submit(self.process_server, instrument_id, server, name)
                for instrument_id, server, name in self.servers
            ]

            for future in as_completed(futures):
                try:
                    future.result()  # Reraises any exceptions
                except Exception as exc:
                    logger.error("UEC import failed: %s", exc)
