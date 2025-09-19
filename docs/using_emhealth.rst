Using EMHealth
==============

Dashboards
----------

Once you login into Grafana at http://localhost:3000, you may want to adjust the default preferences.
Navigate to `Administration > General > Default preferences` where you can set the interface theme, week start etc.
We recommend to set Home Dashboard to **TEM/Fleet overview**.

At the moment, all dashboards are grouped into TEM, SEM and DB performance folders.

TEM dashboards
~~~~~~~~~~~~~~

Overviews
^^^^^^^^^

Fleet overview
``````````````

This is the main dashboard that can display multiple instruments at once. The key metrics are beam time (vacuum state), utilization, last cryo cycle, specimen and data throughput

.. image:: /_static/dash-overview.png

Productivity
````````````

Shows per-instrument counters for autoloader cartridges/cassettes, acquired images and EPU/Tomo sessions

.. image:: /_static/dash-prod.png

Alerts
``````

Provides instrument summary and recent alerts for each microscope module

.. image:: /_static/dash-alerts.png

Modules
^^^^^^^

Autoloader
``````````

Pressure, axes motion, temperatures and LN levels are monitored

.. image:: /_static/dash-al.png

Column
``````

Buffer cycle, cryo cycle frequency and duration, lenses temperature, IGP vacuum, optics board errors are displayed

.. image:: /_static/dash-column.png

Detectors
`````````

Projection vacuum, overal status and sensor temperature for microscope detectors are provided

.. image:: /_static/dash-detectors.png

Motion
``````

Tracks motion errors for stage axes and all apertures

.. image:: /_static/dash-motion.png

PC Health
`````````

Microscope PC statistics

.. image:: /_static/dash-pc.png

Source
``````

Various parameters for FEG and HT are being monitored

.. image:: /_static/dash-source.png

For developers
^^^^^^^^^^^^^^

Data browser
````````````

Is mostly useful for visualizing raw data from the database

.. image:: /_static/dash-browser.png

Import Alarms
-------------

.. note:: This functionality is still in development

Universal Error Codes (UECs) or Alarms from an instrument are stored in a database separate from Health Monitor events and
can be typically displayed with UEC Viewer on the MPC. You could also install *FEI UEC Notifications Exporter* and save UECs to XML,
but this is not supported by ``EMHealth``. If you have the credentials to access the MSSQL server on MPC,
you can import UECs from MSSQL into ``EMHealth`` database. To make it work, MSSQL_USER and MSSQL_PASSWORD have to be defined,
as well as the *server* field for each instrument in the `instruments.json`.

.. code-block::

    emhealth db import-uec
