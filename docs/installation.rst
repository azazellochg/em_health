Installation
------------

FEI/Thermo Fisher Scientific (TFS) electron microscopes store instrument data using `Data Services` software
on the microscope PC (MPC). This data includes event logging (Health Monitor), system configuration, alarms (error codes) etc.
The `Data Services` backend uses Microsoft SQL Server with several databases.

Various applications like Health Monitor, FEI Viewer, UEC viewer, and D2i Data Collector access this data. Since the
`Data Services` API is proprietary and TFS does not provide remote SQL server access, data can only be accessed
through Health Monitor (HM). The HM client is installed on MPC and optionally on support PCs, allowing connection to
`Data Services` to view and export data in XML or CSV formats.

The ``EMHealth`` package provides functionality to:

- Parse and import XML data into a `TimescaleDB <https://docs.tigerdata.com/#TimescaleDB>`_ database
- Visualize and analyze data using `Grafana <https://grafana.com/grafana/>`_

Typical setup
^^^^^^^^^^^^^

1. Windows PC (microscope or support) with:

   - Health Monitor client
   - Scheduled task for continuous data export to a location shared with Linux PC

.. tip:: A single support PC with Health Monitor can connect to different microscopes if they are all on the same network.
   
2. Linux PC running ``EMHealth`` with:

   - Access to the shared directory with exported files
   - Watchdog service monitoring for modified XML files
   - Automatic data import pipeline

Prerequisites
^^^^^^^^^^^^^

Requirements for ``EMHeath`` package:

- `docker <https://docs.docker.com/compose/install/>`_
- `psql <https://www.timescale.com/blog/how-to-install-psql-on-mac-ubuntu-debian-windows>`_
- Python < 3.13
- psycopg
- watchdog
- python-dotenv

Docker and psql should be installed on Linux PC, the rest is managed by conda environment below. It's recommended to
manage docker as non-root user, see details `here <https://docs.docker.com/engine/install/linux-postinstall/>`_

Installation
^^^^^^^^^^^^

1. Set up Python environment and install package:

   .. code-block::

       conda create -y -n emhealth python=3.12
       conda activate emhealth
       git clone https://github.com/azazellochg/em_health
       cd em_health
       pip install -e .

2. Configure and launch containers:

   .. code-block::

       cp .env.example docker/.env  # edit .env with your secrets
       docker compose -f docker/compose.yaml up -d

Security Configuration
^^^^^^^^^^^^^^^^^^^^^^

See .env.example for default values.

- TimescaleDB accounts:

  - POSTGRES_USER (default: *postgres*) - superuser, password: POSTGRES_PASSWORD
  - *grafana* - read-only user, password: POSTGRES_GRAFANA_PASSWORD
  - [optional] *pganalyze* - database metrics user, password: *pganalyze*

- Grafana accounts:

  - *admin* - administrator account, password: GRAFANA_ADMIN_PASSWORD

Data Import
^^^^^^^^^^^

Historical Data Import
~~~~~~~~~~~~~~~~~~~~~~

1. [Windows] Export XML data from Health Monitor (GUI or CLI). Be aware, an instrument can have several associated DataSources (for HM, APM, AutoCTF, AutoStar, ToolReadiness, Velox etc). You need to select one that has `Software->Server` parameter.

a. Choose a date range, e.g. 1 month.
b. Select ALL parameters.
c. Format: XML
d. Press **Save**.

.. image:: /_static/HM_export.png

2. [Recommended] Compress output XML using GZIP (`gzip file.xml`) and transfer file.xml.gz to Linux. This reduces the file size >10 times.
3. Configure instruments in `instruments.json`. See `help <settings.html>`_ for details
4. Import data (this may take a few minutes depending on the number of parameters and amount of data):

   .. code-block::

       emhealth import -i /path/to/file.xml.gz -s em_health/instruments.json

5. If necessary, repeat export and import steps for other instruments.

Automated Import Setup
~~~~~~~~~~~~~~~~~~~~~~

1. Generate Windows batch file for each instrument, the serial number (i.e. 3299 below) should match `instruments.json` file:

   .. code-block::

       emhealth create-task -i 3299 -s em_health/instruments.json

2. Open `3299_export_hm_data.cmd` and change the output (`-f 3299_data.xml`) to a full path pointing to a shared location, available from Linux PC. Make sure the file name terminates with \*_data.xml
3. [Windows] Create a new task in Task Scheduler to trigger the generated script every hour indefinitely. The script will keep overwriting the output xml file. See `help page <task.html>`_ for details

.. note:: The task will run only when a user is logged on. This is because the network drives are mounted on a per-user basis.

4. If necessary, create similar scripts and tasks for other instruments.
5. Start the watchdog service, which checks the directory every 5 minutes for modified files matching \*_data.xml or \*_data.xml.gz:

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
