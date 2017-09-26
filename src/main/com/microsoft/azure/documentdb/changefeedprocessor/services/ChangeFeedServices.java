package com.microsoft.azure.documentdb.changefeedprocessor.services;

import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.changefeedprocessor.IChangeFeedObserverFactory;

/**
 * Controls the
 * - create jobs and start/stop jobs
 * - enumerate partitions in service
 * - lock mechanism for active workers
 */
public class ChangeFeedServices {
    private JobFactory changeFeedJobFactory;
    private PartitionServices partitionServices;
    private LeaseServices leaseServices;

    public ChangeFeedServices(JobFactory changeFeedJobFactory, PartitionServices partitionServices, LeaseServices leaseServices) {
        this.changeFeedJobFactory = changeFeedJobFactory;
        this.partitionServices = partitionServices;
        this.leaseServices = leaseServices;
    }

    public void start() {
        Job j = changeFeedJobFactory.create();

        try {
            j.start(0);

        } catch (DocumentClientException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // create changeFeedJob factory
        // list partitions in services
        // register leases
    }

    public void stop() {
        // stop lease manager
        // stop active workers
    }
}