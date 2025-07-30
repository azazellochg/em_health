Development
-----------

Enable performance metrics
^^^^^^^^^^^^^^^^^^^^^^^^^^

After installation you can enable DB performance monitoring. This is required only for a developer setup: `emhealth db create-perf-stats`.
This will create a separate *pganalyze* account for TimescaleDB and schedule statistics collection.
The output is used in dashboards under *DB performance* folder.

SQL commands
^^^^^^^^^^^^

Below are some frequently used commands for **psql** command-line db client:

* connect: `psql -U postgres -h localhost -d tem`
* change db to sem: `\c sem`
* list all tables: `\d+`
* list table structure: `\d data;`
* list table content: `SELECT * FROM parameters;`
* disconnect: `\q`

Logs
^^^^

All application actions are saved in `emhealth.log`. PostgreSQL logs can be accessed by:

.. code-block::

    docker exec -it --user postgres timescaledb bash
    cd /home/postgres/pgdata/data/log
    cat *.csv

Grafana logs are accessible via `docker logs grafana`
