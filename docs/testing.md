
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

    ? periodically scans for changes
    - the infrastructure is ready

    ? what if partition manager cannot find the collection?
    - it should fail completely

    ? what if partition manager cannot initialize?
    - it should fail completely

    ? what if partition manager cannot find the collection AFTER startup?
    - it should fail if collection is gone
    - network failures: how robust?
    
    ? load balance = orderly shutdown - how to implement?

3. lease services
    - partition manager integration
    a. test the basic communication with ILeaseEvents

    b. test renewer thread
    c. test keeper thread
    d. test pause event

4. Handle split
    a. continuation token for the splitted partition
    b. handle merge 

5. Verify thread safe

? machines with time difference
