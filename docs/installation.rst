Installation
------------

FEI/Thermo Fisher Scientific (TFS) electron microscopes store instrument data using `Data Services` software
on the microscope PC (MPC). This data includes Events/Health Monitor, System Configuration, and Alarms/UEC. 
The `Data Services` backend uses Microsoft SQL Server.

Various applications like Health Monitor, FEI Viewer, and D2i Data Collector access this data. Since TFS 
does not provide remote SQL server credentials, data can only be accessed through Health Monitor (HM). 
The HM client is installed on MPC and optionally on support PCs, allowing connection to
`Data Services` to view and export data in XML or CSV formats.

The ``EM_health`` package provides functionality to:

- Parse and import XML data into a `TimescaleDB <https://docs.tigerdata.com/#TimescaleDB>`_ database
- Visualize and analyze data using `Grafana <https://grafana.com/grafana/>`_

Typical setup
^^^^^^^^^^^^^

1. Windows PC (microscope or support) with:

   - Health Monitor client
   - Scheduled task for continuous data export

.. tip:: One support PC with a Health Monitor can connect to different microscopes if they are all on the same network.
   
2. Linux PC running ``EM_health`` with:

   - Access to the Windows PC file system
   - Watchdog service monitoring for new XML files
   - Automatic data import pipeline

.. note:: Currently supports TEM instruments only.

Prerequisites
^^^^^^^^^^^^^

Windows PC Requirements:

- Health Monitor (GUI and command line client)
- Shared network drive access

Linux PC Requirements:

- `docker <https://docs.docker.com/compose/install/>`_
- `psql <https://www.timescale.com/blog/how-to-install-psql-on-mac-ubuntu-debian-windows>`_
- Python < 3.13
- psycopg2
- watchdog

Installation
^^^^^^^^^^^^

1. Set up Python environment and install package:

   .. code-block::

       conda create -y -n tfs python=3.12
       conda activate tfs
       git clone https://github.com/azazellochg/em_health
       cd em_health
       pip install -e .

2. Configure and launch containers:

   .. code-block::

       cp .env.example docker/.env  # edit .env with your secrets
       sudo docker compose -f docker/docker-compose.yml up -d

Security Configuration
^^^^^^^^^^^^^^^^^^^^^^

Default account setup (see .env.example for defaults):

- TimescaleDB accounts:

  - POSTGRES_USER (default: *postgres*) - superuser, password: POSTGRES_PASSWORD
  - *grafana* - read-only user, password: POSTGRES_GRAFANA_PASSWORD
  - *pganalyze* - database metrics user, password: *pganalyze*

- Grafana account:

  - *admin* - administrator account, password: GRAFANA_ADMIN_PASSWORD

Data Import
^^^^^^^^^^^

Historical Data Import
~~~~~~~~~~~~~~~~~~~~~~

1. Export XML data from Health Monitor (GUI or CLI)
2. [Optional] Compress using GZIP (`gzip file.xml`) and transfer to Linux
3. Configure instruments in `settings.json`. See `help <settings.html>`_ for details
4. Set environment variables:

   .. code-block::

       export POSTGRES_HOST=localhost
       export POSTGRES_USER=postgres
       export POSTGRES_PASSWORD=postgres

5. Import data:

   .. code-block::

       import_xml -i /path/to/file.xml.gz -s em_health/settings.json

Automated Import Setup
~~~~~~~~~~~~~~~~~~~~~~

1. Generate Windows batch file for each instrument, the serial number (i.e. 3299 below) should match `settings.json` file:

   .. code-block::

       create_task -i 3299 -s em_health/settings.json

2. Configure Windows Task Scheduler to run the generated script hourly
3. Start the watchdog service:

   .. code-block::

       watch_xml -i /path/to/xml/dir -s em_health/settings.json

.. note:: Windows scheduled tasks require a user logged in for network drive access. The reason being the network drives are mounted on a per-user basis.

Post-Import Steps
^^^^^^^^^^^^^^^^^

1. Calculate aggregates and materialized views for TimescaleDB:

   .. code-block::

       db_manager -m

2. Access Grafana dashboards at http://localhost:3000

   - Login with admin account
   - Navigate to "TEM" folder for instrument dashboards
