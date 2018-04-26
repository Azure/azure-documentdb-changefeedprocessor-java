package com.microsoft.azure.documentdb.changefeedprocessor.services;

import com.microsoft.azure.documentdb.DocumentClientException;

import java.util.logging.Logger;

public class JobRunnable implements Runnable {	// CR: RunnableJob sounds better.

    private final Job job;
    private String initialData;
    private Logger logger = Logger.getLogger(JobRunnable.class.getName());

    public JobRunnable(Job job, String initialData) {
        this.job = job;
        this.initialData = initialData;

        logger.info("Constructor JobRunnable");
    }

    @Override
    public void run() {
        try {
            logger.info(String.format("Running Job: %s - InitialData: %s", job, initialData));
            job.start(initialData);
        } catch (DocumentClientException e) {
            logger.severe(e.getMessage());
            e.printStackTrace();
        } catch (InterruptedException e) {
            logger.severe(e.getMessage());
            e.printStackTrace();
        }
    }
    
    // CR: eating exceptions.
    // CR: fix all TODOs across the project.
    
    // TODO: stop thread
}
