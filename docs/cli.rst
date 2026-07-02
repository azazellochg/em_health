CLI
===

This section describes ``EMHealth`` commands available through the command-line interface.

.. code::

    emhealth COMMAND arg1 arg2 ...

Main Tasks
----------

Importing Data
~~~~~~~~~~~~~~

Description
^^^^^^^^^^^

Import health monitor data from XML file. Compressed files (\*.xml.gz) are also supported.
Optional `skip-duplicates` argument is useful for small overlapping imports(e.g. automatic import of the last 1h of data every 30 min). If you are importing a large dataset, do not use this
option as it will slow down the process significantly.

Syntax
^^^^^^

.. code-block::

    emhealth import -i /path/to/file.xml.gz -s em_health/instruments.json [--skip-duplicates]

----

Create Windows Batch Script
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Description
^^^^^^^^^^^

Create a Windows batch file to export Health Monitor data. Depending on the HM version, you may need to modify
the executable path via `-e` option. The output `export_hm_data.cmd` file is created in the current directory.

Syntax
^^^^^^

.. code-block::

    emhealth create-task [-e "C:\Program Files (x86)\Thermo Scientific Health Monitor\HealthMonitorCmd.exe"] -s em_health/instruments.json

----

Watchdog
~~~~~~~~

Description
^^^^^^^^^^^

Watch directory for XML file changes and trigger import. The watchdog can import several files in parallel.
Optional `t` argument specifies the polling interval in seconds, the default is 5 minutes.

Syntax
^^^^^^

.. code-block::

    emhealth watch -i /path/to/xml/dir -s em_health/instruments.json [-t 300]

----

Update EMHealth
~~~~~~~~~~~~~~~

Description
^^^^^^^^^^^

Make sure to run `git pull origin master` from the installation folder before running this command. The update command below will migrate the databases schema to the latest
version and update container images.

Syntax
^^^^^^

.. code-block::

    emhealth update

----

Run tests
~~~~~~~~~

Description
^^^^^^^^^^^

Run unit tests to check the XML import parser and import functions. Also executes `pgTAP <https://pgtap.org/>`_ tests on the database.

Syntax
^^^^^^

.. code-block::

    emhealth test

Database Operations
-------------------

Create Data Statistics
~~~~~~~~~~~~~~~~~~~~~~

Description
^^^^^^^^^^^

This command is usually run after you have imported a large batch of historical data. It will aggregate daily
statistics like autoloader counters, EPU/Tomo sessions etc that is used by various dashboards. You only need to run this
command once, the statistics will be refreshed automatically.

Syntax
^^^^^^

.. code-block::

    emhealth db -d tem create-stats

----

Backup
~~~~~~

Description
^^^^^^^^^^^

Perform a logical backup of TimescaleDB (both TEM and SEM) or a physical backup of Grafana databases. The backups are saved into `BACKUP_DIR` folder.

Syntax
^^^^^^

.. code-block::

    emhealth db -d tem backup
    emhealth db -d grafana backup

----

Restore
~~~~~~~

Description
^^^^^^^^^^^

Restore either TimescaleDB or Grafana database from a backup file.

Syntax
^^^^^^

.. code-block::

    emhealth db -d tem restore

----

Erase database
~~~~~~~~~~~~~~

Description
^^^^^^^^^^^

ALL data will be removed from a specified database! Empty tables will be re-initialized.

Syntax
^^^^^^

.. code-block::

    emhealth db -d tem erase

----

Remove Old Data
~~~~~~~~~~~~~~~

Description
^^^^^^^^^^^

Data older than X days will be removed from a database.
Due to the database `design <https://docs2.tigerdata.com/docs/reference/timescaledb/hypertables/drop_chunks>`_, only the data
chunks that are fully within the specified time range will be removed.

Syntax
^^^^^^

.. code-block::

    emhealth db -d tem prune --days 360


Developer Tools
---------------

Reset performance stats
~~~~~~~~~~~~~~~~~~~~~~~

Description
^^^^^^^^^^^

The periodic database statistics collection is enabled by default. Below command can be used if you
modify the pganalyze tables or functions and want to update the jobs. The output is used in dashboards under *DB performance* folder.
Optional `f` flag will erase existing database statistics.

Syntax
^^^^^^

.. code-block::

    emhealth dev -d tem pganalyze [-f]

----

Migrate database
~~~~~~~~~~~~~~~~

Description
^^^^^^^^^^^

Migrate TimescaleDB schema to the latest version (if required).

Syntax
^^^^^^

.. code-block::

    emhealth dev -d tem migrate

----

Import Alarms
~~~~~~~~~~~~~

.. note:: This functionality is currently under development

Universal Error Codes (UECs) or Alarms from an instrument are stored (from TEM server 6.2) in a database separate from Health Monitor events and
can be typically displayed with UEC Viewer. If you have the credentials to access the MSSQL server on MPC,
you can import UECs from MSSQL into ``EMHealth`` database. To make it work, MSSQL_USER and MSSQL_PASSWORD (in the `docker/.env`) have to be defined,
as well as the *server* field with IP address for each instrument in the `instruments.json`.

.. code-block::

    emhealth dev -d tem import-uec

----

Execute queries
~~~~~~~~~~~~~~~

Description
^^^^^^^^^^^

If you have a long query and/or too lazy to use the `psql` client, you can edit **db_analyze.py** and then use the commands below.

Syntax
^^^^^^

.. code-block::

    emhealth dev -d tem run-query
    emhealth dev -d tem explain-query
