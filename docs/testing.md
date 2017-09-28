In the root folder, create a file `app.secrets` with the following settings:

    * COSMOSDB_ENDPOINT=https://<cosmos name>.documents.azure.com:443
    * COSMOSDB_SECRET=<cosmosdb secret>
    * COSMOSDB_DATABASE=<cosmos  database name>
    * COSMOSDB_COLLECTION=<cosmos collection name>
    * COSMOSDB_AUX_COLLECTION=<cosmos auxiliar collection name>

# Getting Started #

Start with the functional testing called `SimpleTest`. It uses the ChangeFeedHost API to create
the host and associate with the observers. The tests are organized per file:

* `ChangeFeedEventHostTest.java`: creates the Change Feed Host and test the API surfaces. It checks for valid constructors and usage.

* `ChangeFeedServicesTest.java`: tests the interaction between Partition and Lease Services. It contains tests for the basic mechanism to synchronize the unleased/leased status.

* `ChangeFeedJobFactoryTest.java`: tests the Change Feed Job thread, and the job factory. It stresses the current functionality to gather data from CosmosDB service and feed the observer in multipartitioned scenarios.

* `PartitionServicesTest.java`: quite simple test related to partition services. It checks partition enumeration.

* `LeaseServicesTest.java`: -- no tests yet --

* `DocumentServiceTest.java`: covers most of the interaction with CosmosDB service, including both partition enumeration and change feed api. 





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
