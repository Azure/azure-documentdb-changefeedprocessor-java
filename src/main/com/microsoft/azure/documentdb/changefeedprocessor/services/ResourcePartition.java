package com.microsoft.azure.documentdb.changefeedprocessor.services;

import com.microsoft.azure.documentdb.DocumentClientException;

public class ResourcePartition {
    private String partitionId;
    private Job resourceJob;

    public ResourcePartition(String partitionId) {
        this.partitionId = partitionId;
        this.resourceJob = null;
    }

    public ResourcePartition(String partitionId, Job resourceJob) {
        this.partitionId = partitionId;
        this.resourceJob = resourceJob;
    }

    public void init(Job resourceJob) {
        this.resourceJob = resourceJob;
    }

    public String getId() {
        return partitionId;
    }

    public void start(JobFactory factory, Object initialData) throws DocumentClientException, InterruptedException {
        resourceJob = factory.create();
        resourceJob.start(initialData);
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
