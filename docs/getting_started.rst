Getting Started
===============

FEI/Thermo Fisher Scientific (TFS) electron microscopes store instrument data using `Data Services` software
on the microscope PC (MPC). This data includes event logging (Health Monitor), system configuration, alarms (error codes) etc.
The `Data Services` backend uses Microsoft SQL Server (and PostgreSQL on new systems) with several databases.

Various applications like Health Monitor, FEI Viewer, UEC viewer, and D2i Data Collector access this data. Since the
`Data Services` API is proprietary and TFS does not provide SQL server credentials, data can only be accessed
through Health Monitor (HM). The HM client is installed on MPC and optionally on support PCs, allowing connection to
`Data Services` to view and export data in XML or CSV formats.

The ``EMHealth`` package provides functionality to:

- Parse and import XML data into a `TimescaleDB <https://docs.tigerdata.com/#TimescaleDB>`_ database
- Visualize and analyze data using `Grafana <https://grafana.com/grafana/>`_

Typical Setup
^^^^^^^^^^^^^

1. Windows PC (microscope or support) with:

   - Health Monitor client
   - Scheduled task for continuous data export to a location shared with Linux PC

.. tip:: A single Health Monitor client (e.g. on a support PC) can connect to different microscopes if they are all on the same network.
   
2. Linux PC running ``EMHealth`` with:

   - Access to the shared directory with exported files
   - Watchdog service monitoring modified XML files
   - Automatic data import pipeline

Prerequisites
^^^^^^^^^^^^^

.. important:: Currently we support Titan Krios and Glacios instruments only.

Requirements for ``EMHealth`` package:

- `docker <https://docs.docker.com/compose/install/>`_
- `psql <https://www.timescale.com/blog/how-to-install-psql-on-mac-ubuntu-debian-windows>`_

The rest is managed by Docker and Conda environment. It's recommended to
manage Docker as a non-root user, see `details <https://docs.docker.com/engine/install/linux-postinstall/>`_

Installation
^^^^^^^^^^^^

1. Set up Python environment and install package:

   .. code-block::

       conda create -y -n emhealth python=3.12
       conda activate emhealth
       git clone https://github.com/azazellochg/em_health
       cd em_health
       pip install -e .

2. Edit the configuration variables in `docker/.env`. Detailed information can be found `here <advanced_setup.html#security-configuration>`_.
3. Launch containers:

   .. code-block::

       docker compose -f docker/compose.yaml up -d

Data Import
^^^^^^^^^^^

Historical Data Import
~~~~~~~~~~~~~~~~~~~~~~

1. [Windows] Export XML data from Health Monitor (via GUI or CLI). Be aware, an instrument can have several associated DataSources (for HM, APM, AutoCTF, AutoStar, ToolReadiness, Velox etc). You need to select one that has `Software->Server` parameter.

a. Choose a date range, e.g. 1 month.
b. Select ALL parameters.
c. Format: XML
d. Press **Save**.

.. image:: /_static/HM_export.png

.. note:: If you select a very large date range, the export may fail. Also, new systems like Krios G4 have thousands of parameters, you should split export to several shorter time ranges.

2. Transfer file.xml to Linux and compress it using GZIP (`gzip file.xml`). This reduces the file size >10 times.
3. Configure instruments in `instruments.json`. See `help <advanced_setup.html#instruments-configuration>`_ for details
4. Import data (this may take a few minutes depending on the number of parameters and amount of data):

   .. code-block::

       emhealth import -i /path/to/file.xml.gz -s em_health/instruments.json

5. If necessary, repeat export and import steps for other instruments.

Automated Import Setup
~~~~~~~~~~~~~~~~~~~~~~

1. Generate Windows batch file for all instruments in the `instruments.json` file:

   .. code-block::

       emhealth create-task -s em_health/instruments.json

2. Open `export_hm_data.cmd` and change **OUTDIR** value to a full path pointing to a shared location, available from Linux PC. Make sure the file name terminates with \*_data.xml
3. [Windows] Create a new task in Task Scheduler to trigger the generated script every 30 min indefinitely. The script will keep overwriting the output xml file. See `advanced setup <advanced_setup.html#creating-a-windows-task>`_ for details

.. note:: The task will run only when a user is logged on. This is because in Windows the network drives are mounted on a per-user basis.

4. Start the watchdog service, which checks the directory every 5 minutes for modified files matching \*_data.xml or \*_data.xml.gz:

   .. code-block::

       emhealth watch -i /path/to/xml/dir -s em_health/instruments.json -t 300

Post-Import Steps
^^^^^^^^^^^^^^^^^

1. Calculate initial historical statistics for the dashboards (run this step only once!):

   .. code-block::

       emhealth db create-stats

2. Access Grafana dashboards at http://localhost:3000

   - Login with *admin* account
   - Navigate to "TEM" folder for instrument dashboards
