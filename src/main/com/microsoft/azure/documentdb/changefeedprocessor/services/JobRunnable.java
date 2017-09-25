package com.microsoft.azure.documentdb.changefeedprocessor.services;

import com.microsoft.azure.documentdb.DocumentClientException;

public class JobRunnable implements Runnable {

    private final Job job;
    private Object initialData;

    public JobRunnable(Job job, Object initialData) {
        this.job = job;
        this.initialData = initialData;
    }

    @Override
    public void run() {
        try {
            job.start(initialData);
        } catch (DocumentClientException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    // TODO: stop thread
}
