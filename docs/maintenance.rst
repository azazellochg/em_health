Backup, restore & update
========================

We provide tools to perform both physical and logical database backups. For physical backups, we use `pgBackRest <https://pgbackrest.org/>`_ installed inside
the Podman container with TimescaleDB. Logical backups are done with standard PostgreSQL tools and can be used to migrate
between major PostgreSQL versions.

Backups are stored in `BACKUP_DIR`. The directory is mounted from the host and has 777 permissions.
We suggest to keep this folder on a separate filesystem, compared to the Podman volumes.

Physical backup
---------------

The default pgBackRest stanza name is *main*. We leave physical backups for the user to setup. Login into the container to manage the backups:

.. code-block::

    podman exec -it emhealth-db bash
    pgbackrest --stanza=main info
    ...

Our suggestion is to setup a cron job on the host to periodically run full and incremental backups:

.. code-block::

    # Full backup every Sunday at 02:00
    0 2 * * 0 /usr/bin/podman exec emhealth-db pgbackrest --stanza=main --type=full backup >> $HOME/emhealth-backup.log 2>&1
    # Incremental backup Monday-Saturday at 02:00
    0 2 * * 1-6 /usr/bin/podman exec emhealth-db pgbackrest --stanza=main --type=incr backup >> $HOME/emhealth-backup.log 2>&1

By default we keep 2 full backups and archive WAL continuously. See `docker/pgbackrest.conf` for details.

To test the point-in-time recovery (PITR):

.. code-block::

    cd em_health
    podman-compose -f docker/compose.yaml down
    podman volume create emhealth_pgdata_restored
    export BACKUP_DIR="..."
    podman run --rm \
        -v emhealth_pgdata_restored:/var/lib/postgresql/18/docker \
        -v "${BACKUP_DIR}:/backups" \
        -v "$(pwd)/docker/pgbackrest.conf:/etc/pgbackrest.conf:ro" \
        --entrypoint pgbackrest \
        ghcr.io/azazellochg/timescaledb:0.1a11 \
        --stanza=main \
        --pg1-path=/var/lib/postgresql/18/docker \
        --type=time \
        --target="2026-10-03 15:36:59+01" \
        --target-action=promote \
        restore

For Grafana, we simply backup its SQLite database file:

.. code-block::

    emhealth db -d grafana backup


Logical backup
--------------

Both TimescaleDB databases can be backed up. For Timescale, we perform a full logical backup with `pg_dump`
which can be used to restore the database between different PostgreSQL versions.

.. code-block::

    emhealth db -d tem backup
    emhealth db -d sem backup

----

Restore a backup
----------------

You can restore either TimescaleDB or Grafana database from a backup file.

.. code-block::

    emhealth db -d tem restore
    emhealth db -d sem restore

Updating
--------

Due to Timescale extension, updating the database might get complicated, we recommend the procedure below:

1. Run `git pull origin master` from the installation folder. This will update the python package and current schema version
2. Run `emhealth update`. For each database, this will try to:

    * backup Grafana db
    * migrate the current tem/sem schemas to the latest version
    * backup tem/sem db
    * delete container volumes
    * pull the latest container images which may contain newer PostgreSQL / Timescale / Grafana versions
    * restore PostgreSQL and Grafana db from the backup
    * upgrade Timescale and other extensions
    * re-run `emhealth dev create-stats` for pganalyze

3. Update historical stats:

.. code-block:: bash

    emhealth db -d tem create-stats
    emhealth db -d sem create-stats

Updating PostgreSQL from v17 to v18
-----------------------------------

Starting from EMHealth 0.1a6 we have migrated PostgreSQL from v17 to v18. Major server version upgrades are not automated, so please follow the steps below:

.. code-block:: bash

    git checkout v0.1a4
    emhealth update
    podman-compose -f docker/compose.yaml down
    podman run --rm -it -v emhealth_pgdata:/var/lib/postgresql/data ghcr.io/azazellochg/timescaledb:0.1a4 bash -c "pg_checksums -D /var/lib/postgresql/data -e -P"
    podman-compose -f docker/compose.yaml up -d
    git checkout v0.1a6
    podman rename timescaledb emhealth-db; podman rename renderer emhealth-renderer; podman rename grafana emhealth-grafana
    emhealth update

The general idea above is to:

a) update extensions to the latest version on PG17
b) enable checksums on the old cluster
c) update EMHealth code
d) rename containers to a new convention
e) make backups
f) start new PG18 and other containers and empty volumes
g) restore old logical backups
h) update extensions on PG18
