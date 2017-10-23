package com.microsoft.azure.documentdb.changefeedprocessor.services;

import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.changefeedprocessor.ChangeFeedObserverCloseReason;

import java.util.logging.Logger;

public class ResourcePartition {
    private String partitionId;
    private Job resourceJob;

    private Logger logger = Logger.getLogger(ResourcePartition.class.getName());

    public ResourcePartition(String partitionId, Job resourceJob) {
        this.partitionId = partitionId;
        this.resourceJob = resourceJob;
    }

    public void start(String initialData) throws DocumentClientException, InterruptedException {
        logger.info(String.format("Starting ResourceParition: PartitionID: %s - InitialData %S", this.partitionId, initialData));
        resourceJob.start(initialData);
    }

    public void stop() {
        resourceJob.stop(ChangeFeedObserverCloseReason.SHUTDOWN);
    }

    public void startJob(Job job) throws DocumentClientException, InterruptedException {
        Object initialData = this.partitionId;
        job.start(initialData);

        this.resourceJob = job;
    }

    public void stopJob() {
        resourceJob.stop();
    }

    public Job getJob() {
        return resourceJob;
    }
}
