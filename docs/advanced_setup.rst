Advanced Setup
==============

Instruments Configuration
^^^^^^^^^^^^^^^^^^^^^^^^^

`instruments.json` file provides main configuration for the microscopes.

.. code-block:: json

    {
        "instrument": "4248, Krios G4 (4.1)",
        "serial": 4248,
        "model": "Titan Krios G4",
        "name": "Krios 4",
        "type": "tem",
        "template": "krios",
        "server": "192.168.76.2"
    }

.. image:: /_static/HM_settings.png

.. important:: An instrument can have several associated DataSources (for HM, APM, AutoCTF, AutoStar, ToolReadiness, Velox etc). You need to select one that has `Software->Server` parameter.

- **instrument**: this field has to match the instrument in the Health Monitor. The format is `"serial number, model name"`. Model name can be found in the Health Monitor, it's the text inside [] brackets (#3 on the screenshot above).
- **serial**: serial number of the instrument. Digits only (#2 on the screenshot above).
- **model**: custom field, used for display purposes. You can customize this.
- **name**: custom field, used for display purposes. You can customize this.
- **type**: database name. "tem" or "sem" only.
- **template**: microscope platform. Allowed values are "krios", "talos" or "sem". Currently not in use.
- **server**: Optional field. IP address of the microscope PC (#1 on the screenshot above). This is used to connect to MSSQL/PostgreSQL servers on the MPC directly.

Security Configuration
^^^^^^^^^^^^^^^^^^^^^^

See `docker/.env` and `docker/secrets/*` files for default values. Environment file only stores non-sensitive configuration variables.

- Database accounts:

  - **postgres** - database superuser, password: see **postgres_pwd** file
  - **emhealth** - r/w user, password: see **postgres_emhealth_pwd**
  - **grafana** - read-only user, password: see **postgres_grafana_pwd**
  - **pganalyze** - database metrics user, password: see **postgres_pganalyze_pwd**
  - [Optional] MSSQL_USER is used to connect to the MSSQL/PostgreSQL databases on the MPC. Password: see **mssql_pwd**

- Grafana accounts:

  - **admin**, password: see **grafana_admin_pwd**

Creating a Windows Task
^^^^^^^^^^^^^^^^^^^^^^^

To create a new task in Windows Task Scheduler:

1. Open Computer Management -> System Tools -> Task Scheduler -> Task Scheduler Library
2. Right click and select `Create New Task`
3. On the *General* tab enter a task name and make sure `Run only when user is logged on` is selected

.. image:: /_static/task-01.png

4. On the *Triggers* tab add a new trigger to repeat the task every 30 min indefinitely

.. image:: /_static/task-02.png

5. On the *Action* tab add a new action to start a program. Enter full path to the created batch script

.. image:: /_static/task-03.png

6. On the *Settings* tab select options which are shown below

.. image:: /_static/task-04.png

7. Right click and run the newly created task to verify it finishes successfully.
