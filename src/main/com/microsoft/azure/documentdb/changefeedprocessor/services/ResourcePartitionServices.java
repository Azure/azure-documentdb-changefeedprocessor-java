package com.microsoft.azure.documentdb.changefeedprocessor.services;

import java.util.Dictionary;
import java.util.Hashtable;

public class ResourcePartitionServices {
    Dictionary<String, ResourcePartition> _resourcePartitions;
    CheckpointServices _checkpointSvcs;

    public ResourcePartitionServices() {
        _resourcePartitions = new Hashtable<>();
        _checkpointSvcs = new CheckpointServices();
    }

    public ResourcePartition create(String partitionId) {
        ResourcePartition resourcePartition = new ResourcePartition(partitionId);

        _resourcePartitions.put(partitionId, resourcePartition);

        return resourcePartition;
    }

    private ResourcePartition get(String partitionId) {
        return _resourcePartitions.get(partitionId);
    }

    public void start(String partitionId) {
        ResourcePartition rp = get(partitionId);

        Object initialData = _checkpointSvcs.getCheckpointData(partitionId);

        rp.start(initialData);
    }
}