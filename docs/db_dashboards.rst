DB performance dashboards
=========================

This set of dashboards is inspired by `Pganalyze <https://pganalyze.com/>`_, `pgwatch <https://pgwat.ch/>`_ and `PostgresAI <https://postgres.ai/>`_
and collects the statistics from existing TEM and SEM databases.

01. Health checks
^^^^^^^^^^^^^^^^^

Health matrix overview for key instance and database metrics

.. image:: /_static/perf/dash-health.png

02. Configuration
^^^^^^^^^^^^^^^^^

- PostgreSQL version and list of extensions
- database configuration options
- TimescaleDB settings

.. image:: /_static/perf/dash-config.png

03. Connections and jobs
^^^^^^^^^^^^^^^^^^^^^^^^

- active connections to the database
- TimescaleDB scheduled jobs details and status
- for a selected job, you can display its schedule and function description

.. image:: /_static/perf/dash-jobs.png

04. Database overview
^^^^^^^^^^^^^^^^^^^^^

- server status, uptime and version
- CPU and RAM utilization
- database size, TimescaleDB rowstore vs columnstore
- queries runtime (QPS) and buffer cache hit ratio
- tuples and transactions rates
- WAL size, locks and temp files

.. image:: /_static/perf/dash-overview.png

05. Schema statistics
^^^^^^^^^^^^^^^^^^^^^

- total number of tables, views, aggregates and hypertables in the database
- hypertable parameters, chunk statistics and compression
- size, columns and chunks statistics for a selected table
- indexes for a selected table and their utilization (scans, hits and reads)
- selected table vacuums and dead rows time series

.. image:: /_static/perf/dash-schemas.png

06. VACUUM analysis
^^^^^^^^^^^^^^^^^^^

- PostgreSQL autovacuum settings
- vacuum counts and other stats for each table
- table and index bloat estimation
- transaction IDs stats to estimate freezing

.. image:: /_static/perf/dash-vacuum.png

07. Views and aggregates
^^^^^^^^^^^^^^^^^^^^^^^^

The list of materialized views and continuous aggregates. For each view you can see its schedule and the function definition

.. image:: /_static/perf/dash-caggs.png

08. Queries
^^^^^^^^^^^

- queries runtime (per call statistics over time)
- queries table with calls and I/O stats
- individual query statistics
- EXPLAIN plans for longest queries

.. image:: /_static/perf/dash-queries.png

09. I/O statistics (pg_stat_io)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- total I/O throughput and times
- I/O ops per second, buffer hit ratio
- reads/writes per backend type
- writebacks, fsync and file extends over time
- buffer cache efficiency

.. image:: /_static/perf/dash-io.png

10. WAL, checkpoints and replication
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- WAL settings
- pg_wal directory size, WAL archival counters
- WAL generation rate and volume
- top queries generating most WAL
- checkpoints statistics
- replication slots statistics

.. image:: /_static/perf/dash-wal.png

11. Host metrics
^^^^^^^^^^^^^^^^

- host OS overview
- CPU, RAM and PGDATA disk usage
- time series for CPU, RAM, disk and I/O usage
- containers resources usage
- Postgres disk usage

.. image:: /_static/perf/dash-host.png
