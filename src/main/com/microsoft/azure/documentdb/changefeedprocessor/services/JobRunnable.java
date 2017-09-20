package com.microsoft.azure.documentdb.changefeedprocessor.services;

public class JobRunnable implements Runnable {

    private final Job _job;
    private Object _initialData;

    public JobRunnable(Job job, Object initialData) {
        this._job = job;
        this._initialData = initialData;
    }

    @Override
    public void run() {
        _job.start(_initialData);
    }

    // TODO: stop thread
}
