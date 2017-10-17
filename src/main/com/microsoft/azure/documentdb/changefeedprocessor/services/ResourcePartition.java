package com.microsoft.azure.documentdb.changefeedprocessor.services;

import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.changefeedprocessor.ChangeFeedObserverCloseReason;

public class ResourcePartition {
    String partitionId;
    Job resourceJob;

    public ResourcePartition(String partitionId, Job resourceJob) {
        this.partitionId = partitionId;
        this.resourceJob = resourceJob;
    }

    public void start(String initialData) throws DocumentClientException, InterruptedException {
        resourceJob.start(initialData);
    }

    public void stop() {
        resourceJob.stop(ChangeFeedObserverCloseReason.SHUTDOWN);
    }

    public Job getJob() {
        return resourceJob;
    }
}
