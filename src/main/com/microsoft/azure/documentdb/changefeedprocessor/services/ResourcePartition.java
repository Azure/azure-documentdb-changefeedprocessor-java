package com.microsoft.azure.documentdb.changefeedprocessor.services;

public class ResourcePartition {
    String _partitionId;
    Job _resourceJob;

    public ResourcePartition(String partitionId, Job resourceJob) {
        _partitionId = partitionId;
        _resourceJob = resourceJob;
    }

    public void start(Object initialData) {
        _resourceJob.start(initialData);
    }

    public void stop() {
        _resourceJob.stop();
    }

    public Job getJob() {
        return _resourceJob;
    }
}
