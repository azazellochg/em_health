Settings
--------

`Settings.json` file provides main configuration for the instruments.

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
   :width: 640 px

- **instrument**: this field has to match the name of the instrument in the Health Monitor. The format is `"serial number, model name"`. Model name can be found in the Health Monitor, it's the text inside [] brackets (#3 on the screenshot above).
- **serial**: serial number of the instrument. Digits only (#2 on the screenshot above).
- **model**: custom field, used for display purposes. You can customize this.
- **name**: custom field, used for display purposes. You can customize this.
- **type**: database name. "tem" or "sem" only.
- **template**: microscope platform. Allowed values are "krios", "talos" or "sem". Currently not in use.
- **server**: IP address of the microscope PC (#1 on the screenshot above).
