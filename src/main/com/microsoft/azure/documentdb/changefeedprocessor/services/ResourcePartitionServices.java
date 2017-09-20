package com.microsoft.azure.documentdb.changefeedprocessor.services;

import java.util.Dictionary;
import java.util.Hashtable;

public class ResourcePartitionServices {
    Dictionary<String, ResourcePartition> _resourcePartitions;

    public ResourcePartitionServices() {
        _resourcePartitions = new Hashtable<>();
    }

    public ResourcePartition create(String partitionId) {
        ResourcePartition resourcePartition = new ResourcePartition(partitionId, new ChangeFeedJob(null));

        _resourcePartitions.put(partitionId, resourcePartition);

        return resourcePartition;
    }

    public ResourcePartition get(String partitionId) {
        return _resourcePartitions.get(partitionId);
    }
}