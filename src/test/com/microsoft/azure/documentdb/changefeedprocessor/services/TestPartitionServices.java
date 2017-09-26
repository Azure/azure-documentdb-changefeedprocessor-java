package com.microsoft.azure.documentdb.changefeedprocessor.services;

import java.util.Arrays;

public class TestPartitionServices implements PartitionServices {
    @Override
    public ResourcePartitionCollection listPartitions() {
        ResourcePartitionCollection partitions = new ResourcePartitionCollection();

        Arrays.stream((new String[]{"1", "2", "3", "4", "5"}))
                .forEach( p -> partitions.put(p, new ResourcePartition(p)) );

        return partitions;
    }
}
