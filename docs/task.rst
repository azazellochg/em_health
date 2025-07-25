Creating a task
---------------

To create a new task in Windows Task Scheduler:

1. Open Computer Management -> System Tools -> Task Scheduler -> Task Scheduler Library
2. Right click and select `Create New Task`
3. On the *General* tab enter a task name and make sure `Run only when user is logged on` is selected

.. image:: /_static/task-01.png

4. On the *Triggers* tab add a new trigger to repeat the task every hour indefinitely

.. image:: /_static/task-02.png

5. On the *Action* tab add a new action to start a program. Enter full path to the created batch script

.. image:: /_static/task-03.png

6. On the *Settings* tab select options which are shown below

.. image:: /_static/task-04.png

7. Right click and run the newly created task to verify it finishes successfully.
