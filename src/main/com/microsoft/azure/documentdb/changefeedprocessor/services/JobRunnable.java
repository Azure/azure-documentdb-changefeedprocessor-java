package com.microsoft.azure.documentdb.changefeedprocessor.services;

public class JobRunnable implements Runnable {

    private final Job job;
    private Object initialData;

    public JobRunnable(Job job, Object initialData) {
        this.job = job;
        this.initialData = initialData;
    }

    @Override
    public void run() {
        job.start(initialData);
    }

    // TODO: stop thread
}
