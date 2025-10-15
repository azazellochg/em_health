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

There are a few limitations:

* You cannot create nested folders for dashboards. Only single level depth is supported.
* You should not rename dashboards or folders via GUI as this will conflict with provisioned files. Do it directly on the files if really needed.
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

All ``EMHealth`` application actions are saved into `emhealth.log`. PostgreSQL logs are in CSV format and can be accessed through:

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
    * instruments - global metadata for each microscope
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
    * stat_snapshots
    * queries
    * sys_stats
    * stat_explains

Measuring ingestion performance
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

These benchmarks compare different ingestion strategies for loading timeseries-like CSV data into TimescaleDB.

Workflow:

1. **Generate test data** with the desired number of rows.
2. **Run one or more ingestion tests** (COPY, EXECUTEMANY, UNNEST).
3. **Compare performance metrics** such as rows/s, query planning, and execution times.

Input dataset
-------------

The input is a simulated CSV file with *N* rows. Each row contains:

- `time` (timestamp, millisecond precision)
- `instrument_id` (integer)
- `param_id` (integer)
- `value_num` (float)
- `value_text` (string, optional)

Data generation parameters:

- 30 days of data
- 10 instruments
- 500–1500 parameters per instrument

To generate 1,000,000 rows:

.. code-block::

    emhealth db test-data 1000000

Benchmarking COPY
-----------------

The **COPY** test uses psycopg3 text-format COPY with a configurable chunk size. Each chunk is a Python string containing concatenated rows. This test allows tuning both chunk size and Postgres server settings.

Run with an 8 MB chunk size:

.. code-block::

    emhealth db test-copy 8388608

Benchmarking EXECUTEMANY
------------------------

The **EXECUTEMANY** test uses `cursor.executemany()` in psycopg3. Internally this leverages libpq’s pipeline mode to run batched `INSERT .. VALUES` statements. We still commit transactions in batches.

Each run inserts *batch_size × num_columns* values.

Example with batch size 1000:

.. code-block::

    emhealth db test-execmany 1000

Benchmarking UNNEST
-------------------

The **UNNEST** test uses `cursor.execute()` to run an `INSERT .. UNNEST` query. Instead of sending row-by-row inserts, this method sends arrays (one per column) and expands them into rows in PostgreSQL. This reduces query planning overhead compared to EXECUTEMANY.

Example with batch size 1000:

.. code-block::

    emhealth db test-unnest 1000

Example output
--------------

Each test is run 5 times. Results include raw wall times, throughput (rows/s), and query planning/execution stats from `pg_stat_statements`.

Example output (truncated):

.. code-block::

    Using insert_copy to insert 997,905 rows into data_staging table:
        Batch size: 8000000
        Raw run times: [0.8793832040391862, 0.903236785903573, 0.8884079209528863, 0.8673364277929068, 0.8408198338001966], rows/s: [1134778.3257815468, 1104809.9629840956, 1123250.903627322, 1150539.7075726986, 1186823.8115766563]
        Avg time over 5 runs: 0.8758 s
        Avg performance: 1,140,040.5423 rows/s
        Calls per run: 1
        Plan time per call: 0.0000 ms
        Exec time per call: 865.8786 ms

Interpreting results
--------------------

- **COPY** is typically the fastest for bulk ingestion. Experiment with chunk sizes (e.g. 4 MB, 8 MB, 16 MB) to balance client/server memory usage.
- **EXECUTEMANY** is slower but more flexible when UPSERTs are required.
- **UNNEST** can outperform EXECUTEMANY for medium batch sizes, since fewer query plans are created.
- Always run with different batch sizes (1,000, 5,000, 10,000) and average results across trials for reliable benchmarks.
