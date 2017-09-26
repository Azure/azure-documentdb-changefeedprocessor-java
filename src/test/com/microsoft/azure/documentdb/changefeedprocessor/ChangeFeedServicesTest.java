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
        PartitionServices partitionServices = new PartitionServices();
        LeaseServices leaseServices = new LeaseServices();

        ChangeFeedServices changeFeedServices = new ChangeFeedServices(factory, partitionServices, leaseServices);

        changeFeedServices.start();

        changeFeedServices.stop();
    }

}
