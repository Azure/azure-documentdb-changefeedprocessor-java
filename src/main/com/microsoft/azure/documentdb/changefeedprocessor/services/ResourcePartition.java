package com.microsoft.azure.documentdb.changefeedprocessor.services;

public class ResourcePartition {
    String _partitionId;
    Job _resourceJob;

    public ResourcePartition(String partitionId, ChangeFeedJob resourceJob) {
        _partitionId = partitionId;
        _resourceJob = resourceJob;
    }

    public void start(Object initialData) {
        _resourceJob.start(initialData);
    }

    public void stop() {
        _resourceJob.stop();
    }
}
