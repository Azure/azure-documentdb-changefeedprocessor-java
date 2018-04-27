package com.microsoft.azure.documentdb.changefeedprocessor;

import com.microsoft.azure.documentdb.DocumentClientException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ResourcePartition {
    private String partitionId;
    private Job resourceJob;
    private Logger logger = LoggerFactory.getLogger(ResourcePartition.class.getName());

    public ResourcePartition(String partitionId, Job resourceJob) {
        this.partitionId = partitionId;
        this.resourceJob = resourceJob;
    }

    public void start(String initialData, DocumentServiceLease dsl) throws DocumentClientException, InterruptedException {
        logger.info(String.format("Starting ResourceParition: PartitionID: %s - InitialData %S", this.partitionId, initialData));
        resourceJob.start(initialData, dsl);
    }

    public void stop() {
        resourceJob.stop(ChangeFeedObserverCloseReason.SHUTDOWN);
    }

    public Job getJob() {
        return resourceJob;
    }
}
