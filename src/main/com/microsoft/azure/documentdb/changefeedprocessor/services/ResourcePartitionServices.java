package com.microsoft.azure.documentdb.changefeedprocessor.services;

import java.util.Dictionary;
import java.util.Hashtable;

public class ResourcePartitionServices {

    Dictionary<String, ResourcePartition> _resourcePartitions;

    public ResourcePartitionServices() {
        _resourcePartitions = new Hashtable<>();
    }

    public ResourcePartition create(String partitionId) {
        ResourcePartition resourcePartition = new ResourcePartition(partitionId);

        _resourcePartitions.put(partitionId, resourcePartition);

        return resourcePartition;
    }

    public void start(String partitionId) {
        System.out.println("Partition started");
    }

    public void stop(String partitionId) {
        System.out.println("Partition stop");
    }
}
