Backup, restore & update
========================

We provide tools to perform both physical and logical database backups. For physical backups, we use `pgBackRest <https://pgbackrest.org/>`_ installed inside
the Docker container with TimescaleDB. Logical backups are done with standard PostgreSQL tools and can be used to migrate
between major PostgreSQL versions.

Backups are stored in `BACKUP_DIR`. The directory is owned by the *postgres* user (uid 999)

Physical backup
---------------

The default pgBackRest stanza name is *main*. We leave physical backups for the user to handle. Login into the container to manage the backups:

.. code-block::

    docker exec -it timescaledb bash
    pgbackrest --stanza=main info
    pgbackrest --stanza=main backup
    ...


By default, we keep 2 full backups + 4 differential backups and 7 days PITR via WAL. See `/etc/pgbackrest/pgbackrest.conf` for details.

To restore the latest physical backup + replay most recent WAL:

.. code-block::

    docker stop timescaledb
    docker volume rm pgdata
    docker volume create pgdata
    docker run --rm -v pgdata:/var/lib/postgresql/data \
        -v ${BACKUP_DIR}:/backups \
        -v ./docker/pgbackrest.conf:/etc/pgbackrest/pgbackrest.conf:ro \
        --entrypoint pgbackrest timescaledb:latest \
        --stanza=main --type=default --target=latest restore


Logical backup
--------------

By default, all TimescaleDB and Grafana databases are backed up. For Timescale, we perform a full logical backup with `pg_dump`
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
2. Run `emhealth update`. For each database, the script will try to:

    * migrate the current db schema to the latest version
    * do the full backup
    * pull the latest container images which may contain newer PostgreSQL / Timescale / Grafana versions
    * restore PostgreSQL and Grafana db from the backup
    * upgrade Timescale and other extensions

3. Update historical stats: `emhealth db -d tem create-stats`

Updating PostgreSQL from v17 to v18
-----------------------------------

Starting from EMHealth 0.1a5 we have migrated PostgreSQL from v17 to v18. Major server version upgrades are not automated, so please follow the steps below:

    - install the latest package that still support PG17: `pip install em_health==0.1a4`
    - update everything except PG17 to the latest version: `emhealth update`
    - upgrade EMHealth to 0.1a5 or later: `pip install -U em_health`
    - update again: `emhealth update`
