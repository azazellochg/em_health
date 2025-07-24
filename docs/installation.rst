Installation
------------

FEI/Thermo Fisher Scientific (TFS) electron microscopes store instrument data using `Data Services` software
on the microscope PC (MPC). This data includes Events/Health Monitor, System Configuration, and Alarms/UEC. 
The `Data Services` backend uses Microsoft SQL Server.

Various applications like Health Monitor, FEI Viewer, UEC viewer, and D2i Data Collector access this data. Since TFS
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
   - Scheduled task for continuous data export to a location shared with Linux PC

.. tip:: A single support PC with Health Monitor can connect to different microscopes if they are all on the same network.
   
2. Linux PC running ``EM_health`` with:

   - Access to the shared directory with exported files
   - Watchdog service monitoring for modified XML files
   - Automatic data import pipeline

Prerequisites
^^^^^^^^^^^^^

Requirements for ``EM_heath`` package:

- `docker <https://docs.docker.com/compose/install/>`_
- `psql <https://www.timescale.com/blog/how-to-install-psql-on-mac-ubuntu-debian-windows>`_
- Python < 3.13
- psycopg
- watchdog

Docker and psql should be installed on Linux PC, the rest is managed by conda environment below.

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
       sudo docker compose -f docker/docker-compose.yml up -d

.. important:: Do NOT change POSTGRES_HOST value in the .env file

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

1. [Windows] Export XML data from Health Monitor (GUI or CLI):

    a. Choose a date range, e.g. 1 month.
    b. Select ALL parameters.
    c. Format: XML
    d. Press **Save**.

    .. image:: /_static/HM_export.png
       :width: 640 px

2. [Recommended] Compress output XML using GZIP (`gzip file.xml`) and transfer file.xml.gz to Linux. This reduces the file size >10 times.
3. Configure instruments in `settings.json`. See `help <settings.html>`_ for details
4. Set environment variables:

   .. code-block::

       export POSTGRES_HOST=localhost
       export POSTGRES_USER=postgres
       export POSTGRES_PASSWORD=postgres

.. note:: The host has to be *localhost*, because we are running the SQL server in a container.

5. Import data (this may take a few minutes depending on the number of parameters and amount of data):

   .. code-block::

       emhealth import -i /path/to/file.xml.gz -s em_health/settings.json

6. If necessary, repeat export and import steps for other instruments.

Automated Import Setup
~~~~~~~~~~~~~~~~~~~~~~

1. Generate Windows batch file for each instrument, the serial number (i.e. 3299 below) should match `settings.json` file:

   .. code-block::

       emhealth create-task -i 3299 -s em_health/settings.json

2. Change the output path (`-f 3299_data.xml`) in the batch script (`3299_export_hm_data.cmd`). Output data to a shared location, available from Linux PC.
3. [Windows] Configure Windows Task Scheduler to run the generated script every hour indefinitely. The script will keep overwriting the output xml file.
4. Start the watchdog service:

   .. code-block::

       emhealth watch-dir -i /path/to/xml/dir -s em_health/settings.json

.. note:: Windows scheduled task requires a user to be logged in for network drive access. The reason being the network drives are mounted on a per-user basis.

Post-Import Steps
^^^^^^^^^^^^^^^^^

1. Calculate initial historical statistics for the dashboards (run this step only once!):

   .. code-block::

       emhealth create-stats

2. Access Grafana dashboards at http://localhost:3000

   - Login with *admin* account
   - Navigate to "TEM" folder for instrument dashboards
