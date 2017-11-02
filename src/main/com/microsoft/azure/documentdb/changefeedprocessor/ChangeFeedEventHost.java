/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.microsoft.azure.documentdb.changefeedprocessor;


import com.microsoft.azure.documentdb.ChangeFeedOptions;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.PartitionKeyRange;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.*;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.documentleasestore.DocumentServiceLease;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.documentleasestore.DocumentServiceLeaseManager;
import com.microsoft.azure.documentdb.changefeedprocessor.services.CheckpointServices;
import com.microsoft.azure.documentdb.changefeedprocessor.services.DocumentServices;
import com.microsoft.azure.documentdb.changefeedprocessor.services.ResourcePartitionServices;

import java.util.Hashtable;
import java.util.List;
import java.util.concurrent.*;
import java.util.logging.Logger;
import java.util.regex.Pattern;

public class ChangeFeedEventHost implements IPartitionObserver<DocumentServiceLease> {

    private final String DefaultUserAgentSuffix = "changefeed-java-0.2";
    private final int DEFAULT_PAGE_SIZE = 100;
    private String hostName;
    private String leasePrefix;
    private DocumentCollectionInfo collectionLocation;
    private DocumentCollectionInfo auxCollectionLocation;
    private ChangeFeedOptions changeFeedOptions;
    private ChangeFeedHostOptions options;
    private PartitionManager<DocumentServiceLease> partitionManager;
    private ILeaseManager<DocumentServiceLease> leaseManager;
    private DocumentServices documentServices;
    private ResourcePartitionServices resourcePartitionSvcs;
    private CheckpointServices checkpointSvcs;
    private IChangeFeedObserverFactory observerFactory;
    private ExecutorService executorService;
    private Logger logger = Logger.getLogger(ChangeFeedEventHost.class.getName());

    public ChangeFeedEventHost( String hostName, DocumentCollectionInfo documentCollectionLocation, DocumentCollectionInfo auxCollectionLocation){
        this(hostName, documentCollectionLocation, auxCollectionLocation, new ChangeFeedOptions(), new ChangeFeedHostOptions());
    }

    public ChangeFeedEventHost(
            String hostName,
            DocumentCollectionInfo documentCollectionLocation,
            DocumentCollectionInfo auxCollectionLocation,
            ChangeFeedOptions changeFeedOptions,
            ChangeFeedHostOptions hostOptions) {

    	if (hostName == null || hostName.isEmpty()) throw new IllegalArgumentException("hostName");
        if (documentCollectionLocation == null) throw new IllegalArgumentException("documentCollectionLocation");
        if (documentCollectionLocation.getUri() == null) throw new IllegalArgumentException("documentCollectionLocation.getUri()");
        if (documentCollectionLocation.getDatabaseName() == null || documentCollectionLocation.getDatabaseName().isEmpty()) throw new IllegalArgumentException("documentCollectionLocation.getDatabaseName() is null or empty");
        if (documentCollectionLocation.getCollectionName() == null || documentCollectionLocation.getCollectionName().isEmpty()) throw new IllegalArgumentException("documentCollectionLocation.getCollectionName() is null or empty");
        if (changeFeedOptions == null) throw new IllegalArgumentException("changeFeedOptions");
        //if (changeFeedOptions.PartitionKeyRangeId != null && !changeFeedOptions.PartitionKeyRangeId.isEmpty()) throw new ArgumentException("changeFeedOptions.PartitionKeyRangeId must be null or empty string");
        if (hostOptions == null) throw new IllegalArgumentException("hostOptions");
        if (hostOptions.getMinPartitionCount() > hostOptions.getMaxPartitionCount()) throw new IllegalArgumentException("hostOptions.MinPartitionCount cannot be greater than hostOptions.MaxPartitionCount");

        this.collectionLocation = canonicalizeCollectionInfo(documentCollectionLocation);
        this.auxCollectionLocation = canonicalizeCollectionInfo(auxCollectionLocation);
        this.changeFeedOptions = changeFeedOptions;
        this.options = hostOptions;
        this.hostName = hostName;

        this.documentServices = new DocumentServices(documentCollectionLocation);
        this.checkpointSvcs = null;
        this.resourcePartitionSvcs = null;

        if (this.changeFeedOptions.getPageSize() == null || this.changeFeedOptions.getPageSize() == 0)
        {
            this.changeFeedOptions.setPageSize(this.DEFAULT_PAGE_SIZE);
        }

        this.executorService = Executors.newFixedThreadPool(1);
    }

    private DocumentCollectionInfo canonicalizeCollectionInfo(DocumentCollectionInfo collectionInfo)
    {
    	// CR: can we do some analog of Debug.Assert() for all private method input check?
    	//     Debug.Assert(collectionInfo != null);
        DocumentCollectionInfo result = collectionInfo;
        if (result.getConnectionPolicy().getUserAgentSuffix() == null ||
                result.getConnectionPolicy().getUserAgentSuffix().isEmpty())
        {
            result = new DocumentCollectionInfo(collectionInfo);
            result.getConnectionPolicy().setUserAgentSuffix(DefaultUserAgentSuffix);
        }

        return result;
    }

    // CR: Can we fix ALL compiler warnings across both projects, so that "Problems" window is clean (currently shows 198 items)?
    //     e.g. this one on registerObserver: Start the 'Infer Generic Type Arguments' refactoring
    
    // CR: remove this comment?
    /**
     * This code used to be async
     */
    public void registerObserver(Class type) throws Exception	// CR: can we use generics?
    {
        logger.info(String.format("Registering Observer of type %s", type));	// CR: change this this.logger for consistency.
        ChangeFeedObserverFactory factory = new ChangeFeedObserverFactory(type);

        registerObserverFactory(factory);

        this.executorService.execute(() -> {
            try {
                start();
            } catch (Exception e) {
                e.printStackTrace();	// CR: does Java support pluggable tracing like .Net (different trace sources, levels, etc)? What is Logger?
            }
        });
    }

    // CR: must have a public registerObserverFactory method, C# version has it and quite a few customers use it.

    // CR: this method is not needed. Remove, just set the factory instead.
    private void registerObserverFactory(ChangeFeedObserverFactory factory) {
        this.observerFactory = factory;
    }

    private void start() throws Exception{
        logger.info(String.format("Starting..."));

        initializeIntegrations();
    }

    private void initializeIntegrations() throws DocumentClientException, LeaseLostException {
        // Grab the options-supplied prefix if present otherwise leave it empty.
        String optionsPrefix = this.options.getLeasePrefix();
        if (optionsPrefix == null) {
            optionsPrefix = "";
        }

        // Beyond this point all access to collection is done via this self link: if collection is removed, we won't access new one using same name by accident.
        this.leasePrefix = String.format("%s%s_%s_%s", optionsPrefix, this.collectionLocation.getUri().getHost(), documentServices.getDatabaseID(), documentServices.getCollectionID());

        this.leaseManager = new DocumentServiceLeaseManager(
                this.auxCollectionLocation,
                this.leasePrefix,
                this.options.getLeaseExpirationInterval(),
                this.options.getLeaseRenewInterval(),
                this.documentServices);

        leaseManager.initialize(true);

        this.checkpointSvcs = new CheckpointServices(this.leaseManager, this.options.getCheckpointFrequency());

        if (this.options.getDiscardExistingLeases()) {
            logger.warning(String.format("Host '%s': removing all leases, as requested by ChangeFeedHostOptions", this.hostName));
            try {
                this.leaseManager.deleteAll().call();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        // Note: lease store is never stale as we use monitored collection Rid as id prefix for aux collection.
        // Collection was removed and re-created, the rid would change.
        // If it's not deleted, it's not stale. If it's deleted, it's not stale as it doesn't exist.
        try {
            this.leaseManager.createLeaseStoreIfNotExists().call();
        } catch (Exception e) {
            e.printStackTrace();
        }

        Hashtable<String, PartitionKeyRange> ranges = this.documentServices.listPartitionRange();

        this.leaseManager.createLeases(ranges);

        logger.info(String.format("Source collection: '%s', %d partition(s), %s document(s)", collectionLocation.getCollectionName(), ranges.size(), documentServices.getDocumentCount()));

        logger.info("Initializing partition manager");
        partitionManager = new PartitionManager<DocumentServiceLease>(this.hostName, this.leaseManager, this.options);
        try {
            this.resourcePartitionSvcs = new ResourcePartitionServices(documentServices, checkpointSvcs, observerFactory, changeFeedOptions.getPageSize());
            partitionManager.subscribe(this).call();
            partitionManager.initialize();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Override
    public Callable<Void> onPartitionAcquired(DocumentServiceLease documentServiceLease) {

        Callable<Void> callable = new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                String[] parts = documentServiceLease.id.split(Pattern.quote("."));
                String partitionId = parts[parts.length-1];
                try {
                    resourcePartitionSvcs.create(partitionId);
                    resourcePartitionSvcs.start(partitionId);
                } catch (DocumentClientException e) {
                    e.printStackTrace();
                }catch (InterruptedException e) {
                    e.printStackTrace();
                }

                return null;
            }
        };
        return callable;

    }

    @Override
    public Callable<Void> onPartitionReleased(DocumentServiceLease documentServiceLease, ChangeFeedObserverCloseReason reason) {

        Callable<Void> callable = new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                String partitionId = documentServiceLease.id;
                logger.info(String.format("Partition id %s finished", partitionId));
                resourcePartitionSvcs.stop(partitionId);
                //TODO:Implement return of callable object
                return null;
            }
        };

        return callable;

    }

    public ExecutorService getExecutorService(){
        return executorService;
    }
}
