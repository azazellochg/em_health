Development
===========

The source code is available at https://github.com/azazellochg/em_health

Changing Dashboards
^^^^^^^^^^^^^^^^^^^

By default, the provisioned dashboards are read-only. If you set **EMHEALTH_DEBUG=true** in the `docker/.env`, you can modify and save changes via the Grafana UI.
However, if you then update the provisioned dashboards (e.g. via `pip install -U em_health`), the changes made via UI will be lost. See details
`here <https://grafana.com/docs/grafana/latest/administration/provisioning/#make-changes-to-a-provisioned-dashboard>`_. The workaround is the following:

1. Make changes to a dashboard via Grafana UI.
2. Save and export dashboard to JSON (DO NOT check `Export the dashboard to use in another instance`).
3. Overwrite existing dashboard file (they are in `docker/grafana/provisioning/dashboards/`) with the saved json file.

Any file changes in the provisioning folder are immediately picked up by Grafana. There's no need to restart it.

There are a few other limitations:

* You cannot create nested folders for dashboards. Only single level depth is supported.
* You should not rename dashboards or folders via GUI as this will conflict with provisioned files. Do it directly on the files if really needed.
* Some provisioned resources (alerts, contact points, datasources) cannot be modified from the GUI. You can create new ones though.


Enable performance metrics
^^^^^^^^^^^^^^^^^^^^^^^^^^

After installation you can enable DB performance monitoring. Generally, this is only required for a developer setup:

.. code-block::

    emhealth db create-perf-stats -f

This will create a separate *pganalyze* account for TimescaleDB and schedule statistics collection.
The output is used in dashboards under *DB performance* folder.

Performance statistics is inspired by `Pganalyze <https://pganalyze.com/>`_ and includes:

* database statistics (updated every 10 min)
* tables statistics (updated every 10 min)
* index statistics (updated every 10 min)
* auto-VACUUM statistics (updated every 1 min)
* query statistics (updated every 1 min)
* CPU and RAM host statistics (updated every 1 min)
* auto-EXPLAIN plans (for queries longer than 500ms)

Statistics retention time is 6 months.

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

Using Grafana API
^^^^^^^^^^^^^^^^^

Grafana provides HTTP API that can be used once you create a `service admin account <http://localhost:3000/org/serviceaccounts/create>`_
with an API token and save it to **GRAFANA_API_TOKEN** in the `docker/.env`. A simple Python client inside ``EMHealth`` can then access the API.
At the moment the client can only change the default organization preferences by running:

.. code-block::

    python em_health/grafana_client.py

Logs
^^^^

All ``EMHealth`` application actions are saved in `emhealth.log`. PostgreSQL logs are in csv format and can be accessed through:

.. code-block::

    docker exec -it postgres timescaledb bash
    cd /var/lib/postgresql/data/log
    cat *.csv

Grafana logs are accessible via:

.. code-block::

    docker logs grafana

Database structure
^^^^^^^^^^^^^^^^^^

We have two databases: *tem* and *sem*, both have the same structure at the moment. Each database has several schemas:

* public - default schema for storing HM events data

    * schema_info - table to store the current schema version
    * instruments - glabal metadata for each microscope
    * enum_types - enumeration names for each instrument
    * enum_values - enumeration values for each enum
    * parameters - parameters metadata
    * enum_values_history - old/replaced enumeration values
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

* fdw_ms_IID - foreign server schema for MSSQL with UECs (for each IID)

    * error_definitions
    * error_notifications

* fdw_pg_IID - foreign server schema for PostgreSQL with HM data (for each IID)

    * event_property
    * event_property_type
    * event_type
    * parameter_type
    * instrument_event_config

* pganalyze - schema to store database statistics for developers

    * database_stats
    * table_stats
    * index_stats
    * vacuum_stats
    * stat_statements
    * stat_snapshots
    * queries
    * sys_stats
    * stat_explains
