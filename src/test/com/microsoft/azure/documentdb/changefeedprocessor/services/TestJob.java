package com.microsoft.azure.documentdb.changefeedprocessor.services;

import com.microsoft.azure.documentdb.DocumentClientException;

public class TestJob implements Job {

    String partitionId;

    @Override
    public void start(Object initialData) throws DocumentClientException, InterruptedException {
        this.partitionId = (String)initialData;
        System.out.println("started partition: " + partitionId);
    }

    @Override
    public void stop() {
        System.out.println("stopped partition: " + partitionId);
    }
}
