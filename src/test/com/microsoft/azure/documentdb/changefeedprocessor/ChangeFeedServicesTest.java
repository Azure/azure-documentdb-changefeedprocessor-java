package com.microsoft.azure.documentdb.changefeedprocessor;

import com.microsoft.azure.documentdb.changefeedprocessor.services.*;
import org.junit.Test;

import static org.junit.Assert.fail;

/**
 * @author fcatae
 *
 */
public class ChangeFeedServicesTest {

    @Test
    public void test() {
        JobFactory factory = new TestChangeFeedJobFactory();
        PartitionServices partitionServices = new TestPartitionServices();
        TestLeaseServices leaseServices = new TestLeaseServices();

        ChangeFeedServices changeFeedServices = new ChangeFeedServices(factory, partitionServices, leaseServices);

        changeFeedServices.start();
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
