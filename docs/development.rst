Development
-----------

Enable performance metrics
^^^^^^^^^^^^^^^^^^^^^^^^^^

After installation you can enable DB performance monitoring. Generally, this is only required for a developer setup: `emhealth db create-perf-stats -f`.
This will create a separate *pganalyze* account for TimescaleDB and schedule statistics collection.
The output is used in dashboards under *DB performance* folder.

Performance statistics is inspired by `Pganalyze <https://pganalyze.com/>`_ and includes:

* database statistics (updated every 10 min)
* tables statistics (updated every 10 min)
* index statistics (updated every 10 min)
* auto-VACUUM statistics (updated every 1 min)
* query statistics (updated every 1 min)
* auto-EXPLAIN plans (for queries longer than 1s)

Statistics retention time is 1 month.

SQL commands
^^^^^^^^^^^^

Below are some frequently used commands for **psql** command-line db client:

* connect: `psql -U postgres -h localhost -d tem`
* change db to sem: `\c sem`
* list tables: `\dt`
* list mat. views: `\dm`
* list table structure: `\d data;`
* list table content: `SELECT * FROM parameters;`
* disconnect: `\q`

Logs
^^^^

All **EMHealth** application actions are saved in `emhealth.log`. PostgreSQL logs can be accessed by:

.. code-block::

    docker exec -it postgres timescaledb bash
    cd /var/lib/postgresql/data/log
    cat *.csv

Grafana logs are accessible via `docker logs grafana`
