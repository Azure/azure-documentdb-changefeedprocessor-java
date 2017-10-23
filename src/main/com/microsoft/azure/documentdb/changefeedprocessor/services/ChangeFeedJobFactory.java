package com.microsoft.azure.documentdb.changefeedprocessor.services;

import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.changefeedprocessor.IChangeFeedObserver;
import com.microsoft.azure.documentdb.changefeedprocessor.IChangeFeedObserverFactory;

public class ChangeFeedJobFactory implements JobFactory {
    private final DocumentServices documentServices;
    private final JobServices jobServices;
    private final CheckpointServices checkpointServices;
    private final IChangeFeedObserverFactory observerFactory;

    public ChangeFeedJobFactory (IChangeFeedObserverFactory observerFactory, DocumentServices documentServices, JobServices jobServices, CheckpointServices checkpointServices) {
        this.observerFactory = observerFactory;
        this.documentServices = documentServices;
        this.jobServices = jobServices;
        this.checkpointServices = checkpointServices;
    }

    @Override
    public Job create() {
        try {
            IChangeFeedObserver observer = observerFactory.createObserver();
            return new ChangeFeedJob2(documentServices, checkpointServices, observer);
        } catch (IllegalAccessException | InstantiationException e) {
            e.printStackTrace();
        }

        return null;
    }

    public void runAsync(Job job, Object initialData) {
        jobServices.runAsync(job, initialData);
    }

}
