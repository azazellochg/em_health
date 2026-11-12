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
from typing import Literal

from em_health.db_manager import DatabaseManager
from em_health.utils.tools import logger


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

            CREATE USER MAPPING IF NOT EXISTS FOR emhealth
            SERVER {name}
            OPTIONS (username {user}, password {password}, row_estimate_method 'showplan_all');
        """, identifiers={"name": self.name}, strings={
            "server": self.server,
            "user": user,
            "password": os.getenv("MSSQL_PASSWORD")
        })

        logger.info("Setup foreign server MSSQL %s@%s:57659 database DS",
                    user, self.server)

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

            CREATE USER MAPPING IF NOT EXISTS FOR emhealth
            SERVER {name}
            OPTIONS (user {user}, password {password}, application_name 'EMHealth');
        """, identifiers={"name": self.name}, strings={
            "server": self.server,
            "user": user,
            "password": os.getenv("MSSQL_PASSWORD")
        })

        logger.info("Setup foreign server PostgreSQL %s@%s:60659 database ds",
                    user, self.server)

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
            LIMIT TO (event_property, event_property_type, event_type, parameter_type, instrument_event_config)
            FROM SERVER {name}
            INTO {schema};
        """, {"schema": self.fdw_schema, "name": self.name})

    def query_pg_events(self):
        """ Query new events from Postgres FDW. """
        return self.dbm.run_query("""
            SELECT
                event_property_type_id AS param_id,
                event_dtm AS time,
                COALESCE(
                    value_float,
                    value_int::double precision,
                    value_bool::int::double precision
                ) AS value_num,
                value_string AS value_text
            FROM {schema}.event_property
            WHERE event_dtm > COALESCE(
                (SELECT MAX(time) FROM events.data WHERE instrument_id = %s),
                '1900-01-01'
            )
        """,
                                  {"schema": self.fdw_schema},
                                  values=(self.instr_id,),
                                  mode="fetchall")

    def query_pg_enums(self):
        """ Query enumerations from Postgres. """

        # Get param_id:enum_name mapping
        params_with_enums = self.dbm.run_query("""
            -- Get config values for existing parameters
            WITH config_xml AS (
                SELECT 
                    regexp_replace(
                        iec.config::text,
                        'xmlns:nil="[^"]*"',
                        ''
                    )::xml AS config
                FROM (
                    SELECT DISTINCT upd_config_id
                    FROM {schema}.event_property_type
                    WHERE is_active = true
                ) ept
                JOIN {schema}.instrument_event_config iec
                    ON iec.instrument_event_config_id = ept.upd_config_id
            )

            -- Extract all Parm nodes with Enum attribute
            SELECT DISTINCT
                (xpath('/Parm/@ID', unnest(xpath('//Instrument//Parm[@Enum]', config))))[1]::text::int AS param_id,
                (xpath('/Parm/@Enum', unnest(xpath('//Instrument//Parm[@Enum]', config))))[1]::text AS enum_name
            FROM config_xml
            ORDER BY param_id
        """, {"schema": self.fdw_schema}, mode="fetchall")

        # Get enum_name:member_name:value mapping
        enum_values = self.dbm.run_query("""
            -- Get config values for existing parameters
            WITH config_xml AS (
                SELECT 
                    regexp_replace(
                        iec.config::text,
                        'xmlns:nil="[^"]*"',
                        ''
                    )::xml AS config
                FROM (
                    SELECT DISTINCT upd_config_id
                    FROM {schema}.event_property_type
                    WHERE is_active = true
                ) ept
                JOIN {schema}.instrument_event_config iec
                    ON iec.instrument_event_config_id = ept.upd_config_id
            ),

            -- Unnest Enums cleanly
            enum_nodes AS (
                SELECT
                    unnest(xpath('//Enum', config)) AS enum_node
                FROM config_xml
            ),

            -- Extract values from each Enum node
            enums AS (
                SELECT
                    (xpath('/Enum/@Name', enum_node))[1]::text AS name,
                    unnest(xpath('/Enum/Value', enum_node)) AS value_node
                FROM enum_nodes
            )

            SELECT DISTINCT
                name,
                (xpath('Value/@Name', value_node))[1]::text AS member_name,
                (xpath('Value/@ID', value_node))[1]::text::int AS value
            FROM enums
            ORDER BY name, value
        """, {"schema": self.fdw_schema}, mode="fetchall")

        return params_with_enums, enum_values

    def query_pg_parameters(self):
        """ Query new parameters data from Postgres.
        A single event (id/name) can be used by multiple parameters (id/name).
        event_property_type_id (param_id) is unique per instrument
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
                ept.absolute_min AS abs_min,
                ept.absolute_max AS abs_max,
                ept.caution_min,
                ept.caution_max,
                ept.warning_min,
                ept.warning_max,
                ept.critical_min,
                ept.critical_max
            FROM {schema}.event_property_type ept
            JOIN {schema}.parameter_type pt ON pt.parameter_type_id = ept.parameter_type_id
            JOIN {schema}.event_type et ON et.event_type_id = ept.event_type_id
            WHERE ept.is_active = true
            ORDER BY ept.event_property_type_id
        """, {"schema": self.fdw_schema}, mode="fetchall")
