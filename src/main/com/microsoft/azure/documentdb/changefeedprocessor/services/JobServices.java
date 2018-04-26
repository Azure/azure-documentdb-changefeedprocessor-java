package com.microsoft.azure.documentdb.changefeedprocessor.services;

public class JobServices {
    public void runAsync(Job job, String initialData) {
        JobRunnable runnable = new JobRunnable(job, initialData);
        Thread thread = new Thread(runnable);
        thread.start();
    }
}
