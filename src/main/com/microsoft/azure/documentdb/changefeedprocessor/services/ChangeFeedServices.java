package com.microsoft.azure.documentdb.changefeedprocessor.services;

import com.microsoft.azure.documentdb.DocumentClientException;

/**
 * Controls the
 * - create jobs and start/stop jobs
 * - enumerate partitions in service
 * - lock mechanism for active workers
 */
public class ChangeFeedServices implements ILeaseSubscriber {
    private final JobFactory changeFeedJobFactory;
    private final PartitionServices partitionServices;
    private final LeaseServices leaseServices;
    private ResourcePartitionCollection activePartitions;

    public ChangeFeedServices(JobFactory changeFeedJobFactory, PartitionServices partitionServices, LeaseServices leaseServices) {
        this.changeFeedJobFactory = changeFeedJobFactory;
        this.partitionServices = partitionServices;
        this.leaseServices = leaseServices;
        this.activePartitions = ResourcePartitionCollection.Empty;
    }

    public void start() {
        // get the current partitions
        ResourcePartitionCollection partitions = partitionServices.listPartitions();

        // subscribe to lease services
        leaseServices.subscribe(this);

        // register the partition in lease services
        for(ResourcePartition p : partitions) {
            leaseServices.register(p);
        }

        activePartitions = partitions;
    }

    public void stop() {
        for(ResourcePartition p : activePartitions) {
            leaseServices.unregister(p);
        }
    }

    public void rescan() {
        // get the current partitions
        ResourcePartitionCollection latestSnapshot = partitionServices.listPartitions();

        // calculate differences
        ResourcePartitionCollection addedPartitions = latestSnapshot.minus(activePartitions);
        ResourcePartitionCollection removedPartitions = activePartitions.minus(latestSnapshot);

        // added partitions
        for(ResourcePartition p : addedPartitions) {
            leaseServices.register(p);
        }

        // removed partitions
        for(ResourcePartition p : removedPartitions) {
            leaseServices.unregister(p);
        }
    }

    @Override
    public void onLeaseAcquired(ResourcePartition partition) {
        Job job = changeFeedJobFactory.create();
        try {
            partition.startJob(job);
        } catch (DocumentClientException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void onLeaseReleased(ResourcePartition partition) {
        partition.stopJob();
    }
}