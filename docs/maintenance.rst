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

    docker exec -it emhealth-db bash
    pgbackrest --stanza=main info
    pgbackrest --stanza=main backup
    ...


By default, we keep 2 full backups + 4 differential backups and 7 days PITR via WAL. See `/etc/pgbackrest/pgbackrest.conf` for details.

To restore the latest physical backup + replay most recent WAL:

.. code-block::

    docker stop emhealth-db
    docker volume rm pgdata
    docker volume create pgdata
    docker run --rm -v pgdata:/var/lib/postgresql/data \
        -v ${BACKUP_DIR}:/backups \
        -v ./docker/pgbackrest.conf:/etc/pgbackrest/pgbackrest.conf:ro \
        --entrypoint pgbackrest emhealth-db:latest \
        --stanza=main --type=default --target=latest restore


Logical backup
--------------

Both TimescaleDB and Grafana databases can be backed up. For Timescale, we perform a full logical backup with `pg_dump`
which can be used to restore the database between different PostgreSQL versions. For Grafana, we simply backup its SQLite database file.

.. code-block::

    emhealth db -d tem backup
    emhealth db -d grafana backup

----

Restore a logical backup
------------------------

You can restore either TimescaleDB or Grafana database from a backup file.

.. code-block::

    emhealth db -d tem restore

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

Starting from EMHealth 0.1a6 we have migrated PostgreSQL from v17 to v18. Major server version upgrades are not automated, so please follow the steps below:

.. code-block:: bash

    pip install em_health==0.1a4
    emhealth update
    docker compose -f docker/compose.yaml down
    docker run --rm -it -v emhealth_pgdata:/var/lib/postgresql/data ghcr.io/azazellochg/timescaledb:0.1a4 bash -c "pg_checksums -D /var/lib/postgresql/data -e -P"
    docker compose -f docker/compose.yaml up -d
    pip install -U em_health
    docker rename timescaledb emhealth-db; docker rename renderer emhealth-renderer; docker rename grafana emhealth-grafana
    emhealth update

The general idea above is to:

a) update extensions to the latest version on PG17,
b) enable checksums on the old cluster,
c) update EMHealth code,
d) rename containers to a new convention,
e) make backups,
f) start new PG18 and other containers and empty volumes
g) restore old logical backups
h) update extensions on PG18
