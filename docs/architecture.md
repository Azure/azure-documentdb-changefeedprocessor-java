Services
=========

* Change Feed Host API: contains the stable API

* Change Feed Services: controls the components
   - Run jobs (individual job per partition)
   - Enumerate partitions and register
   - Control multi-host execution 

* Change Feed job: run the job

* Partition Services: enumerates the partitions

* Checkpoint Services: handles storing the checkpoint data

* Lease Services: lock mechanism to guarantee active workers


Infrastructure
===============

* Document Services: manages calls to DocumentDB service

* Job Services: manages the multi-thread execution

