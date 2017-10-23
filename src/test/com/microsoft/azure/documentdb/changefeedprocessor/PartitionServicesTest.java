package com.microsoft.azure.documentdb.changefeedprocessor;

import com.microsoft.azure.documentdb.changefeedprocessor.services.*;
import org.junit.Test;

/**
 * @author fcatae
 *
 */
public class PartitionServicesTest {

    @Test
    public void test() {
        PartitionServices partitionServices = new TestPartitionServices();

        ResourcePartitionCollection partitions = partitionServices.listPartitions();

        for( ResourcePartition p : partitions ) {
            System.out.println(p.getId());
        }

    }

}
