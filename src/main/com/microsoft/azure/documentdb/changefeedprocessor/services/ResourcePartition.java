package com.microsoft.azure.documentdb.changefeedprocessor.services;

public class ResourcePartition {
    String partitionId;
    Job resourceJob;

    public ResourcePartition(String partitionId, Job resourceJob) {
        this.partitionId = partitionId;
        this.resourceJob = resourceJob;
    }

    public void start(Object initialData) {
        resourceJob.start(initialData);
    }

    public void stop() {
        resourceJob.stop();
    }

    public Job getJob() {
        return resourceJob;
    }
}
