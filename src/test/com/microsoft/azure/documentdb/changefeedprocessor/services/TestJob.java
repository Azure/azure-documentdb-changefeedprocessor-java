package com.microsoft.azure.documentdb.changefeedprocessor.services;

import com.microsoft.azure.documentdb.DocumentClientException;

public class TestJob implements Job {

    private final TestChangeFeedJobFactory parent;
    String partitionId;
    boolean isRunning;

    public TestJob(TestChangeFeedJobFactory parent) {
        this.parent = parent;
        this.isRunning = false;
    }

    @Override
    public void start(Object initialData) throws DocumentClientException, InterruptedException {
        isRunning = true;
        this.partitionId = (String)initialData;
        System.out.println("started partition: " + partitionId);

        parent.enable(partitionId);
    }

    @Override
    public void stop() {
        isRunning = false;
        System.out.println("stopped partition: " + partitionId);

        parent.disable(partitionId);
    }

    public boolean checkIsRunning() {
        return isRunning;
    }
}
