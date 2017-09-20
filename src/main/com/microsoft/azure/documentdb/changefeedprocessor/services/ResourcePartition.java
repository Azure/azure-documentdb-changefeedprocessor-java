package com.microsoft.azure.documentdb.changefeedprocessor.services;

public class ResourcePartition {
    String _partitionId;
    Job _resourceJob;

    public ResourcePartition(String partitionId) {
        _partitionId = partitionId;
        _resourceJob = new ChangeFeedJob();
    }

    public void start(Object initialData) {
        _resourceJob.start(initialData);
    }

    public void stop() {
        _resourceJob.stop();
    }
}
