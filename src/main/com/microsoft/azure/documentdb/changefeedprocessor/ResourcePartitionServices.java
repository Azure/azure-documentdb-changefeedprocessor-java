package com.microsoft.azure.documentdb.changefeedprocessor;

import com.microsoft.azure.documentdb.DocumentClientException;

import java.util.Enumeration;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.microsoft.azure.documentdb.changefeedprocessor.services.*;

class ResourcePartitionServices {
    private CheckpointServices checkpointSvcs;
    private ConcurrentHashMap<String, ResourcePartition> resourcePartitions;
    private DocumentServices client;
    private LeaseManagerInterface<DocumentServiceLease> leaseMgr;
    private ChangeFeedObserverFactoryInterface factory;
    private int pageSize;
    private Logger logger = LoggerFactory.getLogger(ResourcePartitionServices.class.getName());

    public ResourcePartitionServices(DocumentServices client, CheckpointServices checkpointSvcs, LeaseManagerInterface<DocumentServiceLease> dslm,  ChangeFeedObserverFactoryInterface factory, int pageSize) {

        resourcePartitions = new ConcurrentHashMap<>();
        this.client = client;
        this.checkpointSvcs = checkpointSvcs;
        this.leaseMgr = dslm;
        this.factory = factory;
        this.pageSize = pageSize;
    }

    public ResourcePartition create(String partitionId) {
        logger.info(String.format("Creating job for Partition %s", partitionId));
        Job job = null;
        try {
            job = new ChangeFeedJob(partitionId, client, checkpointSvcs, (DocumentServiceLeaseManager) leaseMgr, factory.createObserver(), pageSize);
        } catch (IllegalAccessException e) {
            logger.error(e.getMessage());
            e.printStackTrace();
        } catch (InstantiationException e) {
            logger.error(e.getMessage());
            e.printStackTrace();
        }
        
        ResourcePartition resourcePartition = new ResourcePartition(partitionId, job);

        logger.info("Adding partition to the resourcePartitions dictionary");
        resourcePartitions.put(partitionId, resourcePartition);

        return resourcePartition;
    }

    private ResourcePartition get(String partitionId) {
        return resourcePartitions.get(partitionId);
    }

    public void start(String partitionId, DocumentServiceLease docSvcLease) throws DocumentClientException, InterruptedException {
        ResourcePartition resourcePartition = this.get(partitionId);
        String initialData = checkpointSvcs.getCheckpointData(partitionId);
        logger.info(String.format("Starting partition %s - Checkpoint %s ",partitionId, initialData));
        resourcePartition.start(initialData, docSvcLease);
    }

    public void stop(String partitionId) {
    	// CR: how do we wait for the stop to finish?
        // TODO: improve it
        ResourcePartition resourcePartition = this.get(partitionId);
        resourcePartition.stop();
    }
    
    public void shutdown() {
        Enumeration<ResourcePartition> resourcePartitionEnum = resourcePartitions.elements();
        while(resourcePartitionEnum.hasMoreElements()) {
            ResourcePartition resourcePartition = resourcePartitionEnum.nextElement();
            resourcePartition.stop();
        }
    }
}