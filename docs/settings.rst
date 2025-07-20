Settings
--------

`Settings.json` file provides main configuration for the instruments.

.. code-block:: json

    {
            "instrument": "3299, Titan KRIOS",
            "serial": 3299,
            "model": "Titan Krios G2",
            "name": "Krios 1",
            "type": "tem",
            "template": "krios",
            "server": "192.168.69.2"
    }

- **instrument**: This filed has to match exactly the name of the instrument in the Health Monitor. The format is "serial number, model name".

.. image::

- **serial**: serial number of the instrument. Digits only.
- **model**: custom field, used for display purposes. You can customize this.
- **name**: custom field, used for display purposes. You can customize this.
- **type**: "tem" or "sem".
- **template**: "krios", "talos" or "sem". Currently not in use.
- **server**: IP address of the microscope PC with Data Services.
