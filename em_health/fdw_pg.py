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
from datetime import datetime, timezone

from em_health.fdw_manager import ForeignServer
from em_health.utils.tools import logger


class PgFdw(ForeignServer):
    """ postgres_fdw server for the microscope Postgres ds database (Health Monitor events in the
    core schema), with foreign tables for the query_pg_* methods.
    The core schema is the same on TEMs and SEMs.
    """
    prefix = "pg"

    def setup_server(self):
        """ Create a foreign data wrapper for a Postgres database. """
        user = os.getenv("MSSQL_USER").lower()

        self.dbm.run_query("""
            CREATE SERVER IF NOT EXISTS {name}
            FOREIGN DATA WRAPPER postgres_fdw
            OPTIONS (
                host {server},
                port '60659',
                dbname 'ds',
                application_name 'EMHealth',
                fetch_size '10000'
            );

            CREATE USER MAPPING IF NOT EXISTS FOR emhealth
            SERVER {name}
            OPTIONS (user {user}, password {password});
        """, identifiers={"name": self.name}, strings={
            "server": self.server,
            "user": user,
            "password": os.getenv("MSSQL_PASSWORD")
        })

        logger.info("Setup foreign server PostgreSQL %s@%s:60659 database ds",
                    user, self.server)

    def create_tables(self):
        """ Create foreign tables for the HM Postgres core schema in the shared fdw schema, named
        <server>_<table> (fdw.pg_<id>_event_property, ...). Not IMPORT FOREIGN SCHEMA: it keeps the remote
        table names, so instruments would collide in one schema, and it copies the remote types citext and
        core.valid_timestamptz, which don't exist here. Only the columns used by the query_pg_* methods are
        declared (postgres_fdw matches columns by name). config is text, not xml: its namespace
        declaration makes Postgres XML functions fail. Recreated every time: a foreign table holds no data.
        """
        tables = {
            "event_property": """
                dbid SMALLINT,
                event_property_type_id INTEGER,
                event_dtm TIMESTAMPTZ,
                value_float DOUBLE PRECISION,
                value_int INTEGER,
                value_string TEXT,
                value_bool BOOLEAN""",
            "event_property_type": """
                dbid SMALLINT,
                event_property_type_id INTEGER,
                event_type_id INTEGER,
                parameter_type_id SMALLINT,
                upd_config_id INTEGER,
                identifying_name TEXT,
                label TEXT,
                storage_units TEXT,
                display_units TEXT,
                absolute_min DOUBLE PRECISION,
                absolute_max DOUBLE PRECISION,
                caution_min DOUBLE PRECISION,
                caution_max DOUBLE PRECISION,
                warning_min DOUBLE PRECISION,
                warning_max DOUBLE PRECISION,
                critical_min DOUBLE PRECISION,
                critical_max DOUBLE PRECISION""",
            "event_type": """
                dbid SMALLINT,
                event_type_id INTEGER,
                instrument_id INTEGER,
                identifying_name TEXT""",
            "parameter_type": """
                parameter_type_id SMALLINT,
                identifying_name TEXT""",
            "instrument_event_config": """
                dbid SMALLINT,
                instrument_event_config_id INTEGER,
                instrument_id INTEGER,
                row_ins_dtm TIMESTAMPTZ,
                config TEXT,
                is_valid BOOLEAN""",
        }
        for remote, columns in tables.items():
            self.dbm.run_query(f"""
                DROP FOREIGN TABLE IF EXISTS {{table}};
                CREATE FOREIGN TABLE {{table}} ({columns}
                ) SERVER {{name}}
                OPTIONS (schema_name 'core', table_name {{remote}});
            """, {"table": self.table(remote), "name": self.name}, strings={"remote": remote})

    def pg_tables(self) -> dict[str, str]:
        """ Placeholders for the foreign tables in the query_pg_* methods. """
        return {"ep_t": self.table("event_property"),
                "ept_t": self.table("event_property_type"),
                "et_t": self.table("event_type"),
                "pt_t": self.table("parameter_type"),
                "iec_t": self.table("instrument_event_config")}

    def query_pg_events(self):
        """ Query new events from Postgres FDW.
        The last imported time is read first and passed as a value, so postgres_fdw sends
        "event_dtm > $1" to the microscope. A subquery on the local events.data can't be pushed down,
        and every run would then fetch the whole remote event_property table.
        """
        # psycopg can't load '-infinity' into a datetime. HM's valid_timestamptz domain starts in 1971.
        last_time = self.dbm.run_query(
            "SELECT MAX(time) FROM events.data WHERE instrument_id = %s",
            values=(self.instr_id,), fetch="one")[0] or datetime(1970, 1, 1, tzinfo=timezone.utc)

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
            FROM {ep_t}
            WHERE event_dtm > %s
        """,
                                  self.pg_tables(),
                                  values=(last_time,),
                                  fetch="all")

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
                    FROM {ept_t}
                ) ept
                JOIN {iec_t} iec
                    ON iec.instrument_event_config_id = ept.upd_config_id
            )

            -- Extract all Parm nodes with Enum attribute
            SELECT DISTINCT
                (xpath('/Parm/@ID', unnest(xpath('//Instrument//Parm[@Enum]', config))))[1]::text::int AS param_id,
                (xpath('/Parm/@Enum', unnest(xpath('//Instrument//Parm[@Enum]', config))))[1]::text AS enum_name
            FROM config_xml
            ORDER BY param_id
        """, self.pg_tables(), fetch="all")

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
                    FROM {ept_t}
                ) ept
                JOIN {iec_t} iec
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
        """, self.pg_tables(), fetch="all")

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
            FROM {ept_t} ept
            JOIN {pt_t} pt ON pt.parameter_type_id = ept.parameter_type_id
            JOIN {et_t} et ON et.event_type_id = ept.event_type_id
            ORDER BY ept.event_property_type_id
        """, self.pg_tables(), fetch="all")
