Services
=========

* Change Feed Host API: contains the stable API

* Change Feed Services: controls the components
   - Run jobs (individual job per partition)
   - Enumerate partitions and register
   - Control multi-host execution 

* Change Feed job: create jobs / stop / start
   - depends on JobServices
   - depends on DocumentDb
   - depends on Checkpoint

* Partition Services: enumerates the partitions

* Checkpoint Services: handles storing the checkpoint data

* Lease Services: lock mechanism to guarantee active workers


Infrastructure
===============

* Document Services: manages calls to DocumentDB service
    - DocumentChangeFeedClient

* Job Services: manages the multi-thread execution

* Checkpoint Services: store data related to the change feed


Important Scenarios
=====================

1. create a single Change Feed Job using a specific DocumentDB
    - no checkpoint data
    - no lease management
    a. basic testing
    b. need to test continuationToken
    - validate documentChangeFeedClient exceptions
    c. check robust client handling (including observable exceptions)

2. use partition manager to enumerate the current partitions
    - use a test lease manager that always acquires successfully
    a. check the partitions are active after lease registration
    b. releasing the lease should immediately stop the partition processing
    - test rescan scenarios 
    c. partition is gone (merge/split)
    - observable processing x lease manager
    d. if lease is released, observable should close immediately

    ? what if partition manager cannot find the collection?
    ? what if partition manager cannot initialize?
    ? periodically scans for changes
    ? load balance = orderly shutdown

3. lease services
    a. test the basic communication with ILeaseEvents
    b. test renewer thread
    c. test keeper thread
    d. test pause event

machines with time difference


Handle split


