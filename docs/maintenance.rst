Backup, restore & update
========================

We provide tools to perform both physical and logical database backups. For physical backups, we use `pgBackRest <https://pgbackrest.org/>`_ installed inside
the Docker container with TimescaleDB. Logical backups are done with standard PostgreSQL tools and can be used to migrate
between major PostgreSQL versions.

Backups are stored in `em_health/docker/backups`. The directory is owned by the *postgres* user (uid 999)

Physical backup
---------------

The default pgBackRest stanza name is *main*. We leave physical backups for the user to handle. Login into the container to manage the backups:

.. code-block::

    docker exec -it timescaledb bash
    pgbackrest --stanza=main info
    pgbackrest --stanza=main backup
    ...


By default, we keep up to 3 full backups. See `/etc/pgbackrest/pgbackrest.conf` for details.

To restore the latest physical backup:

.. code-block::

    docker stop timescaledb
    docker volume rm pgdata
    docker volume create pgdata
    docker run --rm -v pgdata:/var/lib/postgresql/data \
        -v ./docker/backups:/backups \
        -v ./docker/pgbackrest.conf:/etc/pgbackrest/pgbackrest.conf:ro \
        --entrypoint pgbackrest timescaledb:latest --stanza=main restore


Logical backup
--------------

By default, both TimescaleDB and Grafana databases are backed up. For Timescale, we perform a full logical backup with `pg_dump`
which can be used to restore the database between different PostgreSQL versions. For Grafana, we simply backup its SQLite database file.

.. code-block::

    emhealth db backup

----

Restore a logical backup
------------------------

You can restore either TimescaleDB or Grafana database from a backup file.

.. code-block::

    emhealth db restore

Updating
--------

Due to Timescale extension, updating the database might get complicated, we recommend the procedure below:

1. Run `pip install -U em_health`. This will update the python package and current schema version
2. Run `emhealth update`. The script will try to:

    * migrate the current db schema to the latest version
    * do the full backup
    * pull the latest container images which may contain newer PostgreSQL / Timescale / Grafana versions
    * restore PostgreSQL and Grafana db from the backup
    * upgrade Timescale extension
