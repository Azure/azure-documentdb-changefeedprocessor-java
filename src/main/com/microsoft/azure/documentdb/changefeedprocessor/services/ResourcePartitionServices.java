package com.microsoft.azure.documentdb.changefeedprocessor.services;

import com.microsoft.azure.documentdb.changefeedprocessor.IChangeFeedObserverFactory;

import java.util.Dictionary;
import java.util.Hashtable;

public class ResourcePartitionServices {
    JobServices _jobServices;
    CheckpointServices _checkpointSvcs;
    Dictionary<String, ResourcePartition> _resourcePartitions;
    DocumentServices _client;
    IChangeFeedObserverFactory _factory;

    public ResourcePartitionServices(DocumentServices client, CheckpointServices checkpointSvcs, IChangeFeedObserverFactory factory) {

        _resourcePartitions = new Hashtable<>();
        _client = client;
        _checkpointSvcs = checkpointSvcs;
        _factory = factory;
        _jobServices = new JobServices();
    }

    public ResourcePartition create(String partitionId) {
        Job job = new ChangeFeedJob(partitionId, _client, _checkpointSvcs, _factory.createObserver());
        ResourcePartition resourcePartition = new ResourcePartition(partitionId, job);

        _resourcePartitions.put(partitionId, resourcePartition);

        return resourcePartition;
    }

    private ResourcePartition get(String partitionId) {
        return _resourcePartitions.get(partitionId);
    }

    public void start(String partitionId) {
        ResourcePartition resourcePartition = this.get(partitionId);
        Job job = resourcePartition.getJob();
        Object initialData = _checkpointSvcs.getCheckpointData(partitionId);

        _jobServices.runAsync(job, initialData);
    }

    public void stop(String partitionId) {
        // TODO: improve it
        ResourcePartition resourcePartition = this.get(partitionId);
        resourcePartition.stop();
    }
}