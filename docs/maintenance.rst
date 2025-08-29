Backup, restore & update
========================

Backup
------

By default, both TimescaleDB and Grafana databases are backed up. For Timescale, we perform a full logical backup with `pg_dump`
which can be used to restore the database between different PostgreSQL versions. For Grafana, we simply backup its sqlite database file.

The backups are saved into `docker/backups` folder.

.. code-block::

    emhealth db backup

----

Restore
-------

You can restore either TimescaleDB or Grafana database from a backup file.

.. code-block::

    emhealth db restore

Updating
--------

Due to Timescale extension, updating the database might get complicated, we recommend the procedure below:

1. Run `pip install -U em_health`. This will update the python package and current schema version
2. Run `emhealth update`. The script will try to:

    * do the full backup
    * pull the latest container images which may contain newer PostgreSQL / Timescale /Grafana versions
    * restore PostgreSQL and Grafana db from the backup
    * upgrade Timescale extension
    * migrate the restored db schema to the latest version
