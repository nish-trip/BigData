# BigData

This project is called Yet Another Centralized Scheduler. This project aims to centrally
control the scheduling of Map and reduce jobs by the master to its n number of workers.
In this case, one master and three workers are used. The Master process makes scheduling
decisions and accordingly passes off tasks to the Worker processes and informs the
Master when a task completes its execution.
The Worker processes listen for Task Launch messages from the Master. On receiving a
launch message, the Worker adds the task to the execution pool of the machine it runs
on.In this case, we will work with one master and three workers. The scheduling
framework receives job requests and launches the tasks in the jobs on machines in the
cluster. Different scheduling algorithms like round-robin, random, least-loaded can be
used. The model aims to simulate the central control of task scheduling done by the
master and the job execution of the workers as well.
