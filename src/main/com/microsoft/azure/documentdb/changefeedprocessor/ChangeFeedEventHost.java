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
import com.microsoft.azure.documentdb.changefeedprocessor.services.CheckpointServices;
import com.microsoft.azure.documentdb.changefeedprocessor.services.DocumentServices;
import com.microsoft.azure.documentdb.changefeedprocessor.services.ResourcePartitionServices;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Logger;

public class ChangeFeedEventHost implements IPartitionObserver<DocumentServiceLease> {

    private final String DefaultUserAgentSuffix = "changefeed-0.2";
    private final String LeaseContainerName = "docdb-changefeed";
    private final String LSNPropertyName = "_lsn";
    private String hostName;
    private String leasePrefix;
    private ConcurrentMap<String, WorkerData> partitionKeyRangeIdToWorkerMap;
    private PartitionManager<DocumentServiceLease> partitionManager;
    private DocumentCollectionInfo collectionLocation;
    private ChangeFeedOptions changeFeedOptions;
    private ChangeFeedHostOptions options;
    private DocumentCollectionInfo auxCollectionLocation;
    private ILeaseManager<DocumentServiceLease> leaseManager;
    private DocumentServices documentServices;
    private ResourcePartitionServices resourcePartitionSvcs;
    private CheckpointServices checkpointSvcs;
    private IChangeFeedObserverFactory observerFactory;
    private final int DEFAULT_PAGE_SIZE = 100;
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

        if (documentCollectionLocation == null) throw new IllegalArgumentException("documentCollectionLocation");
        if (documentCollectionLocation.getUri() == null) throw new IllegalArgumentException("documentCollectionLocation.getUri()");
        if (documentCollectionLocation.getDatabaseName() == null || documentCollectionLocation.getDatabaseName().isEmpty()) throw new IllegalArgumentException("documentCollectionLocation.getDatabaseName() is null or empty");
        if (documentCollectionLocation.getCollectionName() == null || documentCollectionLocation.getCollectionName().isEmpty()) throw new IllegalArgumentException("documentCollectionLocation.getCollectionName() is null or empty");
        if (hostOptions.getMinPartitionCount() > hostOptions.getMaxPartitionCount()) throw new IllegalArgumentException("hostOptions.MinPartitionCount cannot be greater than hostOptions.MaxPartitionCount");

        this.collectionLocation = canonicalizeCollectionInfo(documentCollectionLocation);
        this.changeFeedOptions = changeFeedOptions;
        this.options = hostOptions;
        this.hostName = hostName;
        this.auxCollectionLocation = canonicalizeCollectionInfo(auxCollectionLocation);
        this.partitionKeyRangeIdToWorkerMap = new ConcurrentHashMap<>();

        this.documentServices = new DocumentServices(documentCollectionLocation);
        this.checkpointSvcs = null;

        this.resourcePartitionSvcs = null;

        if (this.changeFeedOptions.getPageSize() == null ||
                this.changeFeedOptions.getPageSize() == 0)
            this.changeFeedOptions.setPageSize(this.DEFAULT_PAGE_SIZE);
    }

    private DocumentCollectionInfo canonicalizeCollectionInfo(DocumentCollectionInfo collectionInfo)
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
        logger.info(String.format("Registering Observer of type %s", type));
        ChangeFeedObserverFactory factory = new ChangeFeedObserverFactory(type);

        registerObserverFactory(factory);
        start();
    }

    private void registerObserverFactory(ChangeFeedObserverFactory factory) {
        this.observerFactory = factory;
    }

    private void start() throws Exception{
        logger.info(String.format("Starting..."));

        initializeIntegrations();

        //initializePartitions();
        //initializeLeaseManager();
    }

    private void initializeIntegrations() throws DocumentClientException, LeaseLostException {
        // Grab the options-supplied prefix if present otherwise leave it empty.
        String optionsPrefix = this.options.getLeasePrefix();
        if( optionsPrefix == null ) {
            optionsPrefix = "";
        }

        // Beyond this point all access to collection is done via this self link: if collection is removed, we won't access new one using same name by accident.
        this.leasePrefix = String.format("%s%s_%s_%s", optionsPrefix, this.collectionLocation.getUri().getHost(), documentServices.getDatabaseID(), documentServices.getCollectionID());

        this.leaseManager = new DocumentServiceLeaseManager(
                this.auxCollectionLocation,
                this.leasePrefix,
                this.options.getLeaseExpirationInterval(),
                this.options.getLeaseRenewInterval());

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

        List<String> range = this.documentServices.listPartitionRange();

//        this.CreateLeases(range);
        this.leaseManager.createLeases(range);

        logger.info(String.format("Source collection: '%s', %d partition(s), %s document(s)", collectionLocation.getCollectionName(), range.size(), documentServices.getDocumentCount()));

        logger.info("Initializing partition manager");
        partitionManager = new PartitionManager<DocumentServiceLease>(this.hostName, this.leaseManager, this.options);
        partitionManager.subscribe(this);
        try {
            partitionManager.initialize();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private void initializePartitions(){
        logger.info("Initializing partitions");

        //TODO: This is not the right place to have this code..
        this.resourcePartitionSvcs = new ResourcePartitionServices(documentServices, checkpointSvcs, observerFactory, changeFeedOptions.getPageSize());

        // list partitions
        List<String> partitionIds = this.listPartition();

        partitionIds.stream().forEach((id) -> {
            logger.info(String.format("PartitionID %s", id));
            resourcePartitionSvcs.create(id);
        });

    }

    private void initializeLeaseManager() {
        // simulate a callback from partitionManager
        hackStartSinglePartition();
    }

    private void hackStartSinglePartition() {
        List<String> partitionIds = this.listPartition();

        partitionIds.stream().forEach((id) -> {
            try {
                resourcePartitionSvcs.start(id);
            } catch (DocumentClientException e) {
                e.printStackTrace();
            }
            catch (InterruptedException e) {
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
    public Callable<Void> onPartitionAcquired(DocumentServiceLease documentServiceLease) {

        Callable<Void> callable = new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                String partitionId = documentServiceLease.id;
                try {
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
}
