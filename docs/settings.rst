Settings
--------

`instruments.json` file provides main configuration for the instruments.

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
- **server**: Optional field. IP address of the microscope PC (#1 on the screenshot above). This is used to connect to MSSQL server on the MPC directly and import UEC.
