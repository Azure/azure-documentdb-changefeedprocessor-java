package com.microsoft.azure.documentdb.changefeedprocessor;

import com.microsoft.azure.documentdb.Resource;
import com.microsoft.azure.documentdb.changefeedprocessor.services.*;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.fail;

/**
 * @author fcatae
 *
 */
public class ChangeFeedServicesTest {

    @Test
    public void testPartitionAndLeaseRegister() {
        JobFactory factory = new TestChangeFeedJobFactory();
        TestPartitionServices partitionServices = new TestPartitionServices();
        TestLeaseServices leaseServices = new TestLeaseServices();

        // create additional partitions -- before changeFeedSetup
        ResourcePartition p1 = partitionServices.add("p1");
        ResourcePartition p2 = partitionServices.add("p2");
        ResourcePartition p3 = partitionServices.add("p3");

        // Setup Change Feed Services
        ChangeFeedServices changeFeedServices = new ChangeFeedServices(factory, partitionServices, leaseServices);
        changeFeedServices.start();

        // create additional partitions -- after changeFeedSetup
        ResourcePartition tp1 = partitionServices.add("tp1");
        ResourcePartition tp2 = partitionServices.add("tp2");
        ResourcePartition tp3 = partitionServices.add("tp3");

        // manually register the partitions -- because they were enrolled after changeFeedSetup
        leaseServices.register(tp1);
        leaseServices.register(tp2);
        leaseServices.register(tp3);

        leaseServices.acquire("tp1");
        leaseServices.acquire("tp3");

        changeFeedServices.stop();
    }


    @Test
    public void testPartitionAndLeaseMechanism() {
        TestChangeFeedJobFactory factory = new TestChangeFeedJobFactory();
        TestPartitionServices partitionServices = new TestPartitionServices();
        TestAutomaticLeaseServices leaseServices = new TestAutomaticLeaseServices(false);

        // create additional partitions -- before changeFeedSetup
        ResourcePartition p1 = partitionServices.add("p1");
        ResourcePartition p2 = partitionServices.add("p2");
        ResourcePartition p3 = partitionServices.add("p3");

        // Setup Change Feed Services
        ChangeFeedServices changeFeedServices = new ChangeFeedServices(factory, partitionServices, leaseServices);
        changeFeedServices.start();

        // no worker threads
        int expectNoWorkerCount = factory.count();
        Assert.assertEquals(expectNoWorkerCount, 0);

        // manually start the worker threads
        leaseServices.acquire("p1");
        leaseServices.acquire("p3");

        // expect to have active workers
        Assert.assertEquals( factory.isActive("p1"), true);
        Assert.assertEquals( factory.isActive("p3"), true);

        // however, partition p2 is still DISABLE
        Assert.assertEquals( factory.isActive("p2"), false);

        changeFeedServices.stop();

        // after shutdown, all partitions are disabled
        Assert.assertEquals( factory.count(), 0 );
    }

    @Test
    public void testPartitionAndLeaseMechanism2() {
        TestChangeFeedJobFactory factory = new TestChangeFeedJobFactory();
        TestPartitionServices partitionServices = new TestPartitionServices();
        TestAutomaticLeaseServices leaseServices = new TestAutomaticLeaseServices(false);

        // create additional partitions -- before changeFeedSetup
        ResourcePartition p1 = partitionServices.add("p1");
        ResourcePartition p2 = partitionServices.add("p2");
        ResourcePartition p3 = partitionServices.add("p3");

        // Setup Change Feed Services
        ChangeFeedServices changeFeedServices = new ChangeFeedServices(factory, partitionServices, leaseServices);
        changeFeedServices.start();

        // no worker threads
        int expectNoWorkerCount = factory.count();
        Assert.assertEquals(expectNoWorkerCount, 0);

        // manually start the worker threads
        leaseServices.acquire("p1");
        leaseServices.acquire("p3");

        // expect to have active workers
        Assert.assertEquals( factory.isActive("p1"), true);
        Assert.assertEquals( factory.isActive("p3"), true);

        // however, partition p2 is still DISABLE
        Assert.assertEquals( factory.isActive("p2"), false);

        // rescan
        changeFeedServices.rescan();

        // acquired a non-existant partition (split?)
        leaseServices.acquire("tp-gone");

        changeFeedServices.stop();
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
