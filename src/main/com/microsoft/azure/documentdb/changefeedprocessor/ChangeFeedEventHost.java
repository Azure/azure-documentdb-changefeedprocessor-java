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
import com.microsoft.azure.documentdb.changefeedprocessor.services.CheckpointDocDbServices;
import com.microsoft.azure.documentdb.changefeedprocessor.services.CheckpointServices;
import com.microsoft.azure.documentdb.changefeedprocessor.services.DocumentServices;
import com.microsoft.azure.documentdb.changefeedprocessor.services.ResourcePartitionServices;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ChangeFeedEventHost implements IPartitionObserver<DocumentServiceLease> {

    private final String DefaultUserAgentSuffix = "changefeed-0.2";
    private final String LeaseContainerName = "docdb-changefeed";
    private final String LSNPropertyName = "_lsn";

    private DocumentCollectionInfo collectionLocation;
    private ChangeFeedOptions changeFeedOptions;
    private ChangeFeedHostOptions options;
    private String hostName;
    private String leasePrefix;
    DocumentCollectionInfo auxCollectionLocation;
    //ConcurrentMap<String, WorkerData> partitionKeyRangeIdToWorkerMap;
    PartitionManager<DocumentServiceLease> partitionManager;
    ILeaseManager<DocumentServiceLease> leaseManager;

    DocumentServices documentServices;
    ResourcePartitionServices resourcePartitionSvcs;
    CheckpointServices checkpointSvcs;

    private IChangeFeedObserverFactory observerFactory;
    private final int DEFAULT_PAGE_SIZE = 100;

    public ChangeFeedEventHost(
            String hostName,
            DocumentCollectionInfo documentCollectionLocation,
            DocumentCollectionInfo auxCollectionLocation){
        this(hostName, documentCollectionLocation, auxCollectionLocation, new ChangeFeedOptions(), new ChangeFeedHostOptions());
    }

    public ChangeFeedEventHost(
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

        this.collectionLocation = CanoninicalizeCollectionInfo(documentCollectionLocation);
        this.changeFeedOptions = changeFeedOptions;
        this.options = hostOptions;
        this.hostName = hostName;
        this.auxCollectionLocation = CanoninicalizeCollectionInfo(auxCollectionLocation);
        //this.partitionKeyRangeIdToWorkerMap = new ConcurrentHashMap<>();

        // initiate services
        this.documentServices = new DocumentServices(this.collectionLocation);
        this.checkpointSvcs = new CheckpointDocDbServices();

        this.resourcePartitionSvcs = null;

        // changeFeedOptions returns Integer (not int), which can be nullable
        if (this.changeFeedOptions.getPageSize() == null || this.changeFeedOptions.getPageSize() == 0)
            this.changeFeedOptions.setPageSize(this.DEFAULT_PAGE_SIZE);
    }

    private DocumentCollectionInfo CanoninicalizeCollectionInfo(DocumentCollectionInfo collectionInfo)
    {
        DocumentCollectionInfo result = collectionInfo;
        if (result.getConnectionPolicy().getUserAgentSuffix() == null ||
                result.getConnectionPolicy().getUserAgentSuffix().isEmpty())
        {
            result = new DocumentCollectionInfo(collectionInfo);
            result.getConnectionPolicy().setUserAgentSuffix(DefaultUserAgentSuffix);
        }

        return result;
    }

    /**
     * This code used to be async
     */
    public void registerObserver(Class type) throws Exception
    {
        ChangeFeedObserverFactory factory = new ChangeFeedObserverFactory(type);

        registerObserverFactory(factory);
        start();
    }

    void registerObserverFactory(ChangeFeedObserverFactory factory) {
        this.observerFactory = factory;
    }

    void start() throws Exception{

        //TODO: This is not the right place to have this code..
        this.resourcePartitionSvcs = new ResourcePartitionServices(documentServices, checkpointSvcs, observerFactory, changeFeedOptions.getPageSize());

        //initializeIntegrations();
        initializePartitions();
        initializeLeaseManager();
    }

    void initializeIntegrations() throws DocumentClientException, LeaseLostException {
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

        leaseManager.initialize();

        this.leaseManager = leaseManager;

//        this.checkpointSvcs = new Refactor.CheckpointServices((ICheckpointManager)leaseManager, this.options.CheckpointFrequency);

        if (this.options.getDiscardExistingLeases()) {
            //TraceLog.Warning(string.Format("Host '{0}': removing all leases, as requested by ChangeFeedHostOptions", this.HostName));
            this.leaseManager.deleteAll();
        }

        // Note: lease store is never stale as we use monitored colleciton Rid as id prefix for aux collection.
        // Collection was removed and re-created, the rid would change.
        // If it's not deleted, it's not stale. If it's deleted, it's not stale as it doesn't exist.
        this.leaseManager.createLeaseStoreIfNotExists();

        List<String> range = this.documentServices.listPartitionRange();

//        TraceLog.Informational(string.Format("Source collection: '{0}', {1} partition(s), {2} document(s)", docdb.CollectionName, range.Count, docdb.DocumentCount));

//        this.CreateLeases(range);

//        this.partitionManager = new PartitionManager<DocumentServiceLease>(this.HostName, this.leaseManager, this.options);
//        await this.partitionManager.SubscribeAsync(this);
//        await this.partitionManager.InitializeAsync();
    }

    void initializePartitions(){
        // list partitions
        List<String> partitionIds = this.listPartition();

        partitionIds.stream().forEach((id) -> {
            resourcePartitionSvcs.create(id);
        });

    }

    void initializeLeaseManager() {
        // simulate a callback from partitionManager
        hackStartSinglePartition();
    }

    void hackStartSinglePartition() {
        List<String> partitionIds = this.listPartition();

        partitionIds.stream().forEach((id) -> {
            try {
                resourcePartitionSvcs.start(id);
            } catch (DocumentClientException e) {
                e.printStackTrace();
            }
        });

    }

    private List listPartition(){
        DocumentServices client = this.documentServices;

        List list = (List)client.listPartitionRange();

        return list;
    }

    @Override
    public void onPartitionAcquired(DocumentServiceLease documentServiceLease) {
        String partitionId = documentServiceLease.id;

        try {
            resourcePartitionSvcs.start(partitionId);
        } catch (DocumentClientException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void onPartitionReleased(DocumentServiceLease documentServiceLease, ChangeFeedObserverCloseReason reason) {
        String partitionId = documentServiceLease.id;

        System.out.println("Partition finished");

        resourcePartitionSvcs.stop(partitionId);
    }
}
