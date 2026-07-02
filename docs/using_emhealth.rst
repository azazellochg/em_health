Using EMHealth
==============

Dashboards
----------

Once you login into Grafana at http://localhost:3000, you may want to adjust the default preferences.
Navigate to `Administration > General > Default preferences` where you can set the interface theme, week start etc.
We recommend to set Home Dashboard to **TEM/Fleet overview**.

At the moment, all dashboards are grouped into separate folders:

- :doc:`TEM dashboards <tem_dashboards>`
- :doc:`SEM dashboards <sem_dashboards>`
- :doc:`DB performance dashboards <db_dashboards>`

Alerting
--------

This functionality is currently under development.

Export data and reporting
-------------------------

You have several options available here:

1. To export raw data from a panel to CSV/Excel format, select `Inspect > Data`. The new window allows you to configure export options and download CSV

.. image:: /_static/export-csv.png

2. To render PNG image of a panel, select `Share > Share link`. In the new window you can customize the image size, then Generate and Download image.

.. image:: /_static/export-png.png

3. To export a whole dashboard as a PNG image, select `Export > Export as image`.
