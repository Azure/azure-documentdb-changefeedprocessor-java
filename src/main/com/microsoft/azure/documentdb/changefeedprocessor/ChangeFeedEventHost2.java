/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.microsoft.azure.documentdb.changefeedprocessor;


import com.microsoft.azure.documentdb.ChangeFeedOptions;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.*;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.documentleasestore.DocumentServiceLease;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.documentleasestore.DocumentServiceLeaseManager;
import com.microsoft.azure.documentdb.changefeedprocessor.services.*;

import java.util.List;
import java.util.logging.Logger;

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

    //integration
    private DocumentServiceLeaseManager leaseManager;
    private PartitionManager<DocumentServiceLease> partitionManager;

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

        // pageSize is Integer (not int), which can be null
        if (this.changeFeedOptions.getPageSize() == null || this.changeFeedOptions.getPageSize() == 0)
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

    void initializeIntegrations(DocumentServices documentServices) throws DocumentClientException, LeaseLostException {
        // Grab the options-supplied prefix if present otherwise leave it empty.
        String optionsPrefix = this.options.getLeasePrefix();
        if( optionsPrefix == null ) {
            optionsPrefix = "";
        }

        // Beyond this point all access to collection is done via this self link: if collection is removed, we won't access new one using same name by accident.
        // this.leasePrefix = String.format("{%s}{%s}_{%s}_{%s}", optionsPrefix, this.collectionLocation.Uri.Host, docdb.DatabaseResourceId, docdb.CollectionResourceId);

        DocumentServiceLeaseManager leaseManager = new DocumentServiceLeaseManager(
                this.auxCollectionLocation,
                this.leasePrefix,
                this.options.getLeaseExpirationInterval(),
                this.options.getLeaseRenewInterval());

        List<String> range = documentServices.listPartitionRange();

//        TraceLog.Informational(string.Format("Source collection: '{0}', {1} partition(s), {2} document(s)", docdb.CollectionName, range.Count, docdb.DocumentCount));

//        this.CreateLeases(range);

        this.partitionManager = new PartitionManager<DocumentServiceLease>(this.hostName, this.leaseManager, this.options);
    }

    private ChangeFeedServices setupChangeFeedServices(DocumentCollectionInfo colInfo, Class type) throws Exception {
        // setup infrastructure components
        ChangeFeedObserverFactory observerFactory = new ChangeFeedObserverFactory(type);
        DocumentServices documentServices = new DocumentServices(colInfo);
        JobServices jobServices = new JobServices();
        CheckpointServices checkpointServices = new CheckpointInMemoryServices();

        // setup the main components
        JobFactory changeFeedJobFactory = new ChangeFeedJobFactory(observerFactory, documentServices, jobServices, checkpointServices);
        PartitionServices partitionServices = new PartitionDocumentServices(documentServices);

        // FOR TESTING ONLY
        // LeaseServices leaseServices = new LeaseDocDbServices(leaseManager, partitionManager, options);
        LeaseServices leaseServices = new SimpleLeaseServices(true);

        return new ChangeFeedServices(changeFeedJobFactory, partitionServices, leaseServices);
    }
}
