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

Syntax
^^^^^^

.. code-block::

    emhealth import -i /path/to/file.xml.gz -s em_health/instruments.json

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

Create Systemd unit file to watch a directory for XML file changes and trigger import. The watchdog can import several files in parallel.
Optional `t` argument specifies the polling interval in seconds, the default is 5 minutes.

Syntax
^^^^^^

.. code-block::

    emhealth create-watcher -i /path/to/xml/dir -s em_health/instruments.json [-t 300]

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
Tests are run on a live tem/sem databases and create a temporary test instrument which is later deleted.

Syntax
^^^^^^

.. code-block::

    emhealth test

Statistics
----------

Description
^^^^^^^^^^^

The dashboards read daily statistics like autoloader counters, EPU/Tomo sessions, pressures etc. from continuous
aggregates (caggs), materialized views and views. Create them once after you have imported a large batch of
historical data, they are refreshed automatically afterwards.

Each command takes view names (the ``events`` schema is optional) or ``all`` (every view of the database), and
works out the dependencies between the views:

* ``list``: the views with their kind, whether they exist, their refresh schedule and the views they read from.
* ``create``: creates the views that are missing, and the missing views they read from. Existing views are skipped,
  so ``create all`` can be rerun at any time (e.g. after an update that adds a view).
* ``create -f``: drops and recreates the views and all views that read from them, e.g. after a view definition
  has changed. ``create -f all`` recreates everything.
* ``drop``: drops the views and all views that read from them. ``create all`` brings them back.
* ``refresh``: refreshes the views now (caggs over the whole history), then the views that read from them,
  e.g. after importing historical data.

Syntax
^^^^^^

.. code-block::

    emhealth stats -d tem list
    emhealth stats -d tem create all
    emhealth stats -d sem create -f sem_beamtime_daily
    emhealth stats -d tem drop epu_sessions
    emhealth stats -d tem refresh epu_sessions

----

Database Operations
-------------------

Migrate database
~~~~~~~~~~~~~~~~

Description
^^^^^^^^^^^

Migrate TimescaleDB schema to the latest version (if required). This command should be only used by developers.

Syntax
^^^^^^

.. code-block::

    emhealth db -d tem migrate
    emhealth db -d sem migrate

----

Backup
~~~~~~

Description
^^^^^^^^^^^

Perform a logical backup of TimescaleDB (either TEM or SEM) or a physical backup of Grafana databases. The backups are saved into `BACKUP_DIR` folder.

Syntax
^^^^^^

.. code-block::

    emhealth db -d tem backup
    emhealth db -d sem backup
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
    emhealth db -d sem restore

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
    emhealth db -d sem erase

----

Remove Old Data
~~~~~~~~~~~~~~~

Description
^^^^^^^^^^^

Data older than X days will be removed from a database.
Due to the database `design <https://docs2.tigerdata.com/docs/reference/timescaledb/hypertables/drop_chunks>`_, only the data
chunks that are fully within the specified time range will be removed. For the UEC codes, prune will delete individual rows by exact time.

Syntax
^^^^^^

.. code-block::

    emhealth db -d tem prune --days 360
    emhealth db -d sem prune --days 360


Developer Tools
---------------

Reset performance stats
~~~~~~~~~~~~~~~~~~~~~~~

Description
^^^^^^^^^^^

The periodic database statistics collection is enabled by default. Below command can be used if you
modify the pganalyze tables or functions and want to update the jobs and views. The output is used in dashboards under *DB performance* folder.
Optional `f` flag will erase existing database statistics.

Syntax
^^^^^^

.. code-block::

    emhealth dev -d tem create-stats [-f]
    emhealth dev -d sem create-stats [-f]

----

Import Alarms
~~~~~~~~~~~~~

.. note:: This functionality is currently under development

Universal Error Codes (UECs) or Alarms from an instrument are stored (from TEM server 6.2) in a database separate from Health Monitor events and
can be typically displayed with UEC Viewer. If you have the credentials to access the MSSQL server on MPC,
you can import UECs from MSSQL into ``EMHealth`` database. To make it work, MSSQL_USER and MSSQL_PASSWORD (in the `docker/.env`) have to be defined,
as well as the *server* field with IP address for each instrument in the `instruments.json`. UECs only exist for TEM instruments.

.. code-block::

    emhealth dev -d tem import-uec

----

Create Service
~~~~~~~~~~~~~~

Create Systemd unit file for host OS statistics collector which populates `pganalyze.sys_info` and `pganalyze.sys_stats` tables.
Optionally, you can change the default time intervals (in seconds) for metrics collection.

.. code-block::

    emhealth dev create-service [-f FAST] [-s SLOW]
