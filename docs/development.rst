Development
===========

The source code is available at https://github.com/azazellochg/em_health

Changing Dashboards
^^^^^^^^^^^^^^^^^^^

By default, the provisioned dashboards are read-only. If you set **EMHEALTH_DEBUG=true** in the `docker/.env`, you can modify and save changes via the Grafana UI.
However, if you then update the provisioned dashboards (e.g. via `git pull`), the changes made via UI will be lost. See details
`here <https://grafana.com/docs/grafana/latest/administration/provisioning/#make-changes-to-a-provisioned-dashboard>`_. The workaround is the following:

1. Make changes to a dashboard via Grafana UI.
2. Save and export dashboard to JSON using `Export > Export as code`, DO NOT toggle `Share dashboard with another instance`.
3. Overwrite existing dashboard file (they are in `docker/grafana/provisioning/dashboards/`) with the saved json file.

Any file changes in the provisioning folder are immediately picked up by Grafana. There's no need to restart it.

There are a few limitations:

* You can create nested folders for dashboards. `Max level depth is 4 <https://grafana.com/docs/grafana/latest/administration/provisioning/#provision-folders-structure-from-filesystem-to-grafana>`_.
* You should not rename dashboards or folders via GUI as this will conflict with provisioned files. Do it directly in the files if really needed.
* Some provisioned resources (alerts, contact points, datasources) cannot be modified from the GUI. You can create new ones though.


DB performance metrics
^^^^^^^^^^^^^^^^^^^^^^

After installation the DB performance monitoring is enabled by default.
You can check the dashboards under *DB performance* folder.

Performance statistics is inspired by `Pganalyze <https://pganalyze.com/>`_ and includes:

* database statistics (updated every 10 min)
* tables statistics (updated every 10 min)
* index statistics (updated every 10 min)
* auto-VACUUM statistics (updated every 1 min)
* query statistics (updated every 1 min)
* CPU and RAM host statistics (updated every 1 min)
* auto-EXPLAIN plans (for queries longer than 500ms)

Statistics retention time is 3 months.

SQL commands
^^^^^^^^^^^^

Below are some frequently used commands for **psql** database client:

* connect: `psql -U postgres -h localhost -d tem`
* change db to sem: `\\c sem`
* list tables: `\\dt`
* list materialized views: `\dm`
* list table structure: `\\d data;`
* list table content: `SELECT * FROM parameters;`
* disconnect: `\\q`

For more examples refer to the command line `cheetsheet <https://gist.github.com/Kartones/dd3ff5ec5ea238d4c546>`_

Logs
^^^^

All ``EMHealth`` application actions are saved into `emhealth.log`. PostgreSQL logs are in CSV format and can be accessed through:

.. code-block::

    docker exec -it emhealth-db bash
    cd /var/lib/postgresql/data/log
    cat *.csv

Grafana logs are accessible via:

.. code-block::

    docker logs emhealth-grafana

Database structure
^^^^^^^^^^^^^^^^^^

We have two databases: *tem* and *sem*, both have the same structure at the moment. Each database has several schemas:

* public - default schema

    * schema_info - table to store the current schema version

* events - schema for storing HM events data

    * configurations - parameters/enumeration dicts
    * instruments - global metadata for each microscope
    * enum_types - enumeration names for each instrument
    * enum_values - enumeration values for each enum
    * parameters - parameters metadata
    * parameters_history - old/replaced parameters
    * data - main events data table for all instruments
    * data_staging - staging table for bulk data inserts with COPY

* uec - schema for storing UECs / Alarms. UEC codes are unified across different instruments

    * device_type
    * device_instance
    * error_code
    * subsystem
    * error_definitions
    * errors - main UEC data table for all instruments

* fdw_ms_IID - foreign server schema for MSSQL with UECs (for each instrument ID)

    * error_definitions
    * error_notifications

* fdw_pg_IID - foreign server schema for PostgreSQL with HM data (for each instrument ID)

    * event_property
    * event_property_type
    * event_type
    * parameter_type
    * instrument_event_config

* pganalyze - schema to store database statistics

    * database_stats
    * table_stats
    * index_stats
    * vacuum_stats
    * stat_statements
    * queries
    * sys_stats
    * stat_explains

Import HM data from PostgreSQL
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. note:: This functionality is currently under development

On a MPC running Windows 10 and TEM server 7.2+, the health monitor data (not UEC) is stored in a PostgreSQL
database and linked to MSSQL using foreign-data wrapper (DSPostgres). The client to import this data directly from MPC is under development.
