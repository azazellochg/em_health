Development
-----------

Enable performance metrics
^^^^^^^^^^^^^^^^^^^^^^^^^^

After installation you can enable DB performance monitoring. This is required only for a developer setup: `db_analyze -p`.
This will create a separate *pganalyze* account for Postgres and schedule statistics collection.
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

All actions are saved in `emhealth.log`
