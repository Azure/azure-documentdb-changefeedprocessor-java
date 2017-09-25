package com.microsoft.azure.documentdb.changefeedprocessor.services;

import com.microsoft.azure.documentdb.DocumentClientException;

public class ResourcePartition {
    String partitionId;
    Job resourceJob;

    public ResourcePartition(String partitionId, Job resourceJob) {
        this.partitionId = partitionId;
        this.resourceJob = resourceJob;
    }

    public void start(Object initialData) throws DocumentClientException, InterruptedException {
        resourceJob.start(initialData);
    }

    public void stop() {
        resourceJob.stop();
    }

    public Job getJob() {
        return resourceJob;
    }
}
