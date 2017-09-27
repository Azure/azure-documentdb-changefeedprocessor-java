package com.microsoft.azure.documentdb.changefeedprocessor;

import com.microsoft.azure.documentdb.Resource;
import com.microsoft.azure.documentdb.changefeedprocessor.services.*;
import org.junit.Test;

import static org.junit.Assert.fail;

/**
 * @author fcatae
 *
 */
public class ChangeFeedServicesTest {

    @Test
    public void testPartitionAndLease() {
        JobFactory factory = new TestChangeFeedJobFactory();
        TestPartitionServices partitionServices = new TestPartitionServices();
        TestLeaseServices leaseServices = new TestLeaseServices();

        // create additional partitions -- before changeFeedSetup
        ResourcePartition p1 = partitionServices.add("p1");
        ResourcePartition p2 = partitionServices.add("p2");
        ResourcePartition p3 = partitionServices.add("p3");

        ChangeFeedServices changeFeedServices = new ChangeFeedServices(factory, partitionServices, leaseServices);

        changeFeedServices.start();

        // create additional partitions
        ResourcePartition tp1 = partitionServices.add("tp1");
        ResourcePartition tp2 = partitionServices.add("tp2");
        ResourcePartition tp3 = partitionServices.add("tp3");

        leaseServices.register(tp1);
        leaseServices.register(tp2);
        leaseServices.register(tp3);

        leaseServices.acquire("tp1");
        leaseServices.acquire("tp3");

        changeFeedServices.stop();

        // acquired a non-existant partition (split?)
        leaseServices.acquire("tp-gone");
    }

    @Test
    public void testChangeFeed() {
        DocumentServices documentServices = null;
        JobServices jobServices = null;
        CheckpointServices checkpointServices = null;
        IChangeFeedObserverFactory observerFactory = null;

        JobFactory factory = new ChangeFeedJobFactory(observerFactory, documentServices, jobServices, checkpointServices );

        PartitionServices partitionServices = new TestPartitionServices();
        TestLeaseServices leaseServices = new TestLeaseServices();

        ChangeFeedServices changeFeedServices = new ChangeFeedServices(factory, partitionServices, leaseServices);

        changeFeedServices.start();
        changeFeedServices.stop();
    }


}
