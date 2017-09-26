package com.microsoft.azure.documentdb.changefeedprocessor.services;

import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.changefeedprocessor.IChangeFeedObserverFactory;

public class ChangeFeedServices {
    private ChangeFeedJobFactory changeFeedJobFactory;
    private PartitionServices partitionServices;
    private LeaseServices leaseServices;

    public ChangeFeedServices(ChangeFeedJobFactory changeFeedJobFactory, PartitionServices partitionServices, LeaseServices leaseServices) {
        this.changeFeedJobFactory = changeFeedJobFactory;
        this.partitionServices = partitionServices;
        this.leaseServices = leaseServices;
    }

    public void start() {

    }

    public void stop() {

    }

    // list the partitions
    // create the jobs
    // enroll in lease services
    // if lease wakes -> call partition.start()
}