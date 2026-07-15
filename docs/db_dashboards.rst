DB performance dashboards
=========================

This set of dashboards is inspired by `Pganalyze <https://pganalyze.com/>`_ and collects the statistics from existing TEM and SEM databases.

Configuration
^^^^^^^^^^^^^

- list of PostgreSQL extensions
- database configuration options
- recommended TimescaleDB settings
- hypertables compression performance

.. image:: /_static/perf/dash-config.png

Connections and jobs
^^^^^^^^^^^^^^^^^^^^

- active connections to the database
- TimescaleDB scheduled jobs details and status
- for a selected job, you can display its schedule and function description

.. image:: /_static/perf/dash-jobs.png

Overview
^^^^^^^^

- server status, uptime and version
- CPU and RAM utilization
- database size, TimescaleDB rowstore vs columnstore
- queries runtime (average execution time per call)
- tuples and transactions rate
- WAL size, locks and temp files

.. image:: /_static/perf/dash-overview.png

Queries
^^^^^^^

- queries runtime (per call statistics over time)
- queries table with calls and I/O stats
- individual query statistics
- EXPLAIN plans for longest queries

.. image:: /_static/perf/dash-queries.png

Schema statistics
^^^^^^^^^^^^^^^^^

- total number of tables, views, aggregates and hypertables in the database
- size, columns and chunks statistics for a selected table
- indexes for a selected table and their utilization (scans, hits and reads)
- table vacuums and dead rows time series

.. image:: /_static/perf/dash-schemas.png

VACUUM analysis
^^^^^^^^^^^^^^^

- PostgreSQL autovacuum settings
- vacuum counts and other stats for each table
- table bloat estimation
- transaction IDs stats to estimate freezing

.. image:: /_static/perf/dash-vacuum.png

Views and aggregates
^^^^^^^^^^^^^^^^^^^^

The list of materialized views and continuous aggregates. For each view you can see its schedule and the function definition

.. image:: /_static/perf/dash-caggs.png
