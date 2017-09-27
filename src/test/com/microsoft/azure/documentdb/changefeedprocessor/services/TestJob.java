package com.microsoft.azure.documentdb.changefeedprocessor.services;

import com.microsoft.azure.documentdb.DocumentClientException;

public class TestJob implements Job {

    private final TestChangeFeedJobFactory parent;
    String partitionId;

    public TestJob(TestChangeFeedJobFactory parent) {
        this.parent = parent;
    }

    @Override
    public void start(Object initialData) throws DocumentClientException, InterruptedException {
        this.partitionId = (String)initialData;
        System.out.println("started partition: " + partitionId);

        parent.enable(partitionId);
    }

    @Override
    public void stop() {
        System.out.println("stopped partition: " + partitionId);

        parent.disable(partitionId);
    }
}
