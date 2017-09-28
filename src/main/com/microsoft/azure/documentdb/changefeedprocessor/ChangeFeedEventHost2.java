/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.microsoft.azure.documentdb.changefeedprocessor;


import com.microsoft.azure.documentdb.ChangeFeedOptions;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.*;
import com.microsoft.azure.documentdb.changefeedprocessor.services.*;

public class ChangeFeedEventHost2 {

    private final String DefaultUserAgentSuffix = "changefeed-0.2";
    private final String LeaseContainerName = "docdb-changefeed";
    private final String LSNPropertyName = "_lsn";
    private final int DEFAULT_PAGE_SIZE = 100;

    private String hostName;
    private DocumentCollectionInfo collectionLocation;
    private DocumentCollectionInfo auxCollectionLocation;
    private String leasePrefix;
    private ChangeFeedOptions changeFeedOptions;
    private ChangeFeedHostOptions options;

    private IChangeFeedObserverFactory observerFactory;

    private ChangeFeedServices changeFeedHost;

    public ChangeFeedEventHost2(
            String hostName,
            DocumentCollectionInfo documentCollectionLocation,
            DocumentCollectionInfo auxCollectionLocation){
        this(hostName, documentCollectionLocation, auxCollectionLocation, new ChangeFeedOptions(), new ChangeFeedHostOptions());
    }

    public ChangeFeedEventHost2(
            String hostName,
            DocumentCollectionInfo documentCollectionLocation,
            DocumentCollectionInfo auxCollectionLocation,
            ChangeFeedOptions changeFeedOptions,
            ChangeFeedHostOptions hostOptions) {

        if (documentCollectionLocation == null) throw new IllegalArgumentException("documentCollectionLocation");
        if (documentCollectionLocation.getUri() == null) throw new IllegalArgumentException("documentCollectionLocation.getUri()");
        if (documentCollectionLocation.getDatabaseName() == null || documentCollectionLocation.getDatabaseName().isEmpty()) throw new IllegalArgumentException("documentCollectionLocation.getDatabaseName() is null or empty");
        if (documentCollectionLocation.getCollectionName() == null || documentCollectionLocation.getCollectionName().isEmpty()) throw new IllegalArgumentException("documentCollectionLocation.getCollectionName() is null or empty");
        if (hostOptions.getMinPartitionCount() > hostOptions.getMaxPartitionCount()) throw new IllegalArgumentException("hostOptions.MinPartitionCount cannot be greater than hostOptions.MaxPartitionCount");

        this.hostName = hostName;
        this.collectionLocation = documentCollectionLocation.getCollectionInfo(DefaultUserAgentSuffix);
        this.auxCollectionLocation = auxCollectionLocation.getCollectionInfo(DefaultUserAgentSuffix);
        this.changeFeedOptions = changeFeedOptions;
        this.options = hostOptions;

        if (this.changeFeedOptions.getPageSize() == 0)
            this.changeFeedOptions.setPageSize(this.DEFAULT_PAGE_SIZE);
    }

    public void registerObserver(Class type) throws Exception {
        if(changeFeedHost != null) {
            throw new Exception("registeredObserver");
        }

        changeFeedHost = setupChangeFeedServices(this.collectionLocation, type);
        changeFeedHost.start();
    }

    public void unregister() throws Exception {
        if (changeFeedHost != null) {
            changeFeedHost.stop();
            changeFeedHost = null;
        }
    }

    private ChangeFeedServices setupChangeFeedServices(DocumentCollectionInfo colInfo, Class type) throws Exception {
        // setup infrastructure components
        ChangeFeedObserverFactory observerFactory = new ChangeFeedObserverFactory(type);
        DocumentServices documentServices = new DocumentServices(colInfo);
        JobServices jobServices = new JobServices();
        CheckpointServices checkpointServices = new CheckpointInMemoryServices();

        // setup the main components
        JobFactory changeFeedJobFactory = new ChangeFeedJobFactory(observerFactory, documentServices, jobServices, checkpointServices);
        PartitionServices partitionServices = null;
        LeaseServices leaseServices = null;

        return new ChangeFeedServices(changeFeedJobFactory, partitionServices, leaseServices);
    }
}
