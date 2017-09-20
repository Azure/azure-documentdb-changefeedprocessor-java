package com.microsoft.azure.documentdb.changefeedprocessor.services;

import java.util.Dictionary;
import java.util.Hashtable;

public class ResourcePartitionServices {
    CheckpointServices _checkpointSvcs;
    Dictionary<String, ResourcePartition> _resourcePartitions;
    DocumentServices _client;

    public ResourcePartitionServices(DocumentServices client, CheckpointServices checkpointSvcs) {

        _resourcePartitions = new Hashtable<>();
        _client = client;
        _checkpointSvcs = checkpointSvcs;
    }

    public ResourcePartition create(String partitionId) {
        Job job = new ChangeFeedJob(partitionId, _client, _checkpointSvcs);
        ResourcePartition resourcePartition = new ResourcePartition(partitionId, job);

        _resourcePartitions.put(partitionId, resourcePartition);

        return resourcePartition;
    }

    private ResourcePartition get(String partitionId) {
        return _resourcePartitions.get(partitionId);
    }

    public void start(String partitionId) {
        ResourcePartition resourcePartition = this.get(partitionId);
        Object initialData = _checkpointSvcs.getCheckpointData(partitionId);
        resourcePartition.start(initialData);
    }

    public void stop(String partitionId) {
        ResourcePartition resourcePartition = this.get(partitionId);
        resourcePartition.stop();
    }
}