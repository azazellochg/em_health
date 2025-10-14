CLI
===

This section describes ``EMHealth`` commands available through the command-line interface.

.. code::

    emhealth [-d DATABASE] COMMAND arg1 arg2 ...

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

Create Aggregated Statistics
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Description
^^^^^^^^^^^

This command is usually run after you have imported a large batch of historical data. It will aggregate daily
statistics like autoloader counters, EPU/Tomo sessions etc that is used by various dashboards. You only need to run this
command once, the statistics will be refreshed automatically every 12h.

Syntax
^^^^^^

.. code-block::

    emhealth db create-stats

----

Remove Old Data
~~~~~~~~~~~~~~~

Description
^^^^^^^^^^^

Erase data for a specific instrument. You must input the serial number that matches `instruments.json`
configuration file. Optional `date` argument can be used to remove data older than **DD-MM-YYYY**.

Syntax
^^^^^^

.. code-block::

    emhealth db clean-inst -i 3299 [--date DATE]


Maintenance Tasks
-----------------

Update EMHealth
~~~~~~~~~~~~~~~

Description
^^^^^^^^^^^

Make sure to run `pip install -U em_health` before running this command. The update script will migrate the database schema to the latest
version and update container images.

Syntax
^^^^^^

.. code-block::

    emhealth update

----

Migrate database
~~~~~~~~~~~~~~~~

Description
^^^^^^^^^^^

Migrate TimescaleDB schema to the latest version (if required).

Syntax
^^^^^^

.. code-block::

    emhealth db migrate

----

Backup
~~~~~~

Description
^^^^^^^^^^^

Perform a logical backup of TimescaleDB and a physical backup of Grafana database. The backups are saved into `docker/backups` folder.

Syntax
^^^^^^

.. code-block::

    emhealth db backup

----

Restore
~~~~~~~

Description
^^^^^^^^^^^

Restore either TimescaleDB or Grafana database from a backup.

Syntax
^^^^^^

.. code-block::

    emhealth db restore

----

Run Tests
~~~~~~~~~

Description
^^^^^^^^^^^

Run unit tests to check the parser and import functions. This will create a temporary dummy instrument record and verify
whether everything works correctly.

Syntax
^^^^^^

.. code-block::

    emhealth test

Developer Commands
------------------

Create performance stats
~~~~~~~~~~~~~~~~~~~~~~~~

Description
^^^^^^^^^^^

The periodic database statistics collection is enabled by default. Below command can be used if you
modify the pganalyze tables or functions and want to update the jobs. The output is used in dashboards under *DB performance* folder.


Syntax
^^^^^^

.. code-block::

    emhealth db create-perf-stats [-f]

Execute queries
~~~~~~~~~~~~~~~

Description
^^^^^^^^^^^

If you have a long query and/or too lazy to use the `psql` client, you can edit **db_analyze.py** and then use the commands below.

Syntax
^^^^^^

.. code-block::

    emhealth db run-query
    emhealth db explain-query
