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
from typing import Literal

from em_health.db_manager import DatabaseManager
from em_health.utils.logs import logger


class FDWManager:
    """ Manager class for foreign data wrappers. """
    def __init__(self,
                 dbm: DatabaseManager,
                 wrapper_type: Literal["tds_fdw", "postgres_fdw"],
                 server: str,
                 instr_id: int):
        self.dbm = dbm
        self.wrapper = wrapper_type
        self.server = server
        self.instr_id = instr_id

        if self.wrapper == "tds_fdw":
            self.name = f"ms_{instr_id}"
            self.setup_fdw_mssql()
            self.fdw_schema = f"fdw_{self.name}"
            self.create_fdw_tables_ms()
        elif self.wrapper == "postgres_fdw":
            self.name = f"pg_{instr_id}"
            self.setup_fdw_postgres()
            self.fdw_schema = f"fdw_{self.name}"
            self.create_fdw_tables_pg()

    def setup_fdw_mssql(self):
        """ Create a foreign data wrapper for a MSSQL database. """
        user = os.getenv("MSSQL_USER")

        self.dbm.run_query("""
            CREATE SERVER IF NOT EXISTS {name}
            FOREIGN DATA WRAPPER tds_fdw
            OPTIONS (
                servername {server},
                port '57659',
                database 'DS'
            );

            CREATE USER MAPPING IF NOT EXISTS FOR public
            SERVER {name}
            OPTIONS (username {user}, password {password});
        """, identifiers={"name": self.name}, strings={
            "server": self.server,
            "user": user,
            "password": os.getenv("MSSQL_PASSWORD")
        })

        logger.info("Connected to MSSQL %s@%s:57659 database DS", user, self.server)

    def setup_fdw_postgres(self):
        """ Create a foreign data wrapper for a Postgres database. """
        user = os.getenv("MSSQL_USER").lower()

        self.dbm.run_query("""
            CREATE SERVER IF NOT EXISTS {name}
            FOREIGN DATA WRAPPER postgres_fdw
            OPTIONS (
                host {server},
                port '60659',
                dbname 'ds'
            );

            CREATE USER MAPPING IF NOT EXISTS FOR public
            SERVER {name}
            OPTIONS (user {user}, password {password});
        """, identifiers={"name": self.name}, strings={
            "server": self.server,
            "user": user,
            "password": os.getenv("MSSQL_PASSWORD")
        })

        logger.info("Connected to PostgreSQL %s@%s:60659 database ds", user, self.server)

    def create_fdw_tables_ms(self):
        """ Create tables for MSSQL FDW. """
        self.dbm.run_query("""
            CREATE SCHEMA IF NOT EXISTS {schema};
            CREATE FOREIGN TABLE IF NOT EXISTS {schema}.error_definitions (
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
        """, {"schema": self.fdw_schema, "name": self.name})

        self.dbm.run_query("""
            CREATE FOREIGN TABLE IF NOT EXISTS {schema}.error_notifications (
                ErrorDtm TIMESTAMPTZ,
                ErrorDefinitionID INTEGER,
                MessageText TEXT
            ) SERVER {name}
            OPTIONS (schema_name 'qry', table_name 'ErrorNotifications');
        """, {"schema": self.fdw_schema, "name": self.name})

    def create_fdw_tables_pg(self):
        """ Create tables for Postgres FDW. """
        self.dbm.run_query("""
            IMPORT FOREIGN SCHEMA core
            LIMIT TO (event_property, event_property_type, event_type, parameter_type)
            FROM SERVER {name}
            INTO {schema};
        """, {"schema": self.fdw_schema, "name": self.name})

    def setup_import_job_ms(self) -> str:
        """ Create a function to import data from the MSSQL database. """
        job_name = f"import_from_{self.name}"

        self.dbm.run_query("""
            DROP FUNCTION IF EXISTS uec.{job};
            CREATE FUNCTION uec.{job}(job_id INT DEFAULT NULL, config JSONB DEFAULT NULL)
            RETURNS void
            LANGUAGE plpgsql
            AS $$
            BEGIN
               -- 1. Device types
                INSERT INTO uec.device_type (DeviceTypeID, IdentifyingName)
                SELECT DISTINCT fdw.DeviceTypeID, fdw.DeviceType
                FROM {schema}.error_definitions fdw
                LEFT JOIN uec.device_type dt ON dt.DeviceTypeID = fdw.DeviceTypeID
                WHERE dt.DeviceTypeID IS NULL;

                -- 2. Device instances
                INSERT INTO uec.device_instance (DeviceInstanceID, DeviceTypeID, IdentifyingName)
                SELECT DISTINCT fdw.DeviceInstanceID, fdw.DeviceTypeID, fdw.DeviceInstance
                FROM {schema}.error_definitions fdw
                LEFT JOIN uec.device_instance di
                    ON di.DeviceInstanceID = fdw.DeviceInstanceID AND di.DeviceTypeID = fdw.DeviceTypeID
                WHERE di.DeviceInstanceID IS NULL;

                -- 3. Error codes
                INSERT INTO uec.error_code (DeviceTypeID, ErrorCodeID, IdentifyingName)
                SELECT DISTINCT fdw.DeviceTypeID, fdw.ErrorCodeID, fdw.ErrorCode
                FROM {schema}.error_definitions fdw
                LEFT JOIN uec.error_code ec
                    ON ec.DeviceTypeID = fdw.DeviceTypeID AND ec.ErrorCodeID = fdw.ErrorCodeID
                WHERE ec.ErrorCodeID IS NULL;

                -- 4. Subsystems
                INSERT INTO uec.subsystem (SubsystemID, IdentifyingName)
                SELECT DISTINCT fdw.SubsystemID, fdw.Subsystem
                FROM {schema}.error_definitions fdw
                LEFT JOIN uec.subsystem ss ON ss.SubsystemID = fdw.SubsystemID
                WHERE ss.SubsystemID IS NULL;

                -- 5. Error definitions
                INSERT INTO uec.error_definitions (ErrorDefinitionID, SubsystemID, DeviceTypeID, ErrorCodeID, DeviceInstanceID)
                SELECT fdw.ErrorDefinitionID, fdw.SubsystemID, fdw.DeviceTypeID, fdw.ErrorCodeID, fdw.DeviceInstanceID
                FROM {schema}.error_definitions fdw
                LEFT JOIN uec.error_definitions ed ON ed.ErrorDefinitionID = fdw.ErrorDefinitionID
                WHERE ed.ErrorDefinitionID IS NULL;

                -- 6. Error notifications
                INSERT INTO uec.errors (Time, InstrumentID, ErrorID, MessageText)
                SELECT
                    fdw.ErrorDtm,
                    {instr_id},
                    ed.ErrorDefinitionID,
                    fdw.MessageText
                FROM {schema}.error_notifications fdw
                JOIN uec.error_definitions ed ON ed.ErrorDefinitionID = fdw.ErrorDefinitionID
                ON CONFLICT (Time, InstrumentID, ErrorID) DO NOTHING;
            END;
            $$;
        """, {"job": job_name, "schema": self.fdw_schema},
                       strings={"instr_id": self.instr_id})

        return job_name

    def query_pg_events(self, since: str = None):
        """ Query events from Postgres FDW. """
        return self.dbm.run_query("""
            SELECT
                event_property_type_id AS param_id,
                event_dtm AS time,
                value_float,
                value_int,
                value_string,
                value_bool
            FROM {schema}.event_property
            WHERE event_dtm > {since}
        """, {"schema": self.fdw_schema}, strings={"since": since}, mode="fetchall")

    def query_pg_parameters(self):
        """ Query parameters data from Postgres.
        A single event (id/name) can be used by multiple parameters (id/name).
        event_property_type_id is unique per instrument
        ept.event_type_id+ept.identifying_name must be unique
        """
        return self.dbm.run_query("""
            SELECT
                ept.event_property_type_id AS param_id,
                ept.event_type_id AS event_id,
                et.identifying_name AS event_name,
                ept.identifying_name AS param_name,
                ept.label AS display_name,
                ept.display_units AS display_unit,
                ept.storage_units AS storage_unit,
                pt.identifying_name AS value_type,
                ept.absolute_min,
                ept.absolute_max,
                ept.caution_min,
                ept.caution_max,
                ept.warning_min,
                ept.warning_max,
                ept.critical_min,
                ept.critical_max,
                ept.is_active AS is_active
            FROM {schema}.event_property_type ept
            JOIN {schema}.parameter_type pt ON pt.parameter_type_id = ept.parameter_type_id
            JOIN {schema}.event_type et ON et.event_type_id = ept.event_type_id
            ORDER BY ept.event_property_type_id
        """, {"schema": self.fdw_schema}, mode="fetchall")
