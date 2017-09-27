package com.microsoft.azure.documentdb.changefeedprocessor.services;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TestPartitionServices implements PartitionServices {

    ArrayList<String> partitionNames;

    public TestPartitionServices() {
        partitionNames = new ArrayList<>();
        partitionNames.add("0");
        partitionNames.add("1");
        partitionNames.add("2");
    }

    @Override
    public ResourcePartitionCollection listPartitions() {
        ResourcePartitionCollection partitions = new ResourcePartitionCollection();

        partitionNames
            .stream()
            .forEach( p -> partitions.put(p, new ResourcePartition(p)) );

        return partitions;
    }

    public ResourcePartition add(String name) {
        partitionNames.add(name);

        return new ResourcePartition(name);
    }
}
