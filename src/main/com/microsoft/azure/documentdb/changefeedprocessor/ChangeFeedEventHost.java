package com.microsoft.azure.documentdb.changefeedprocessor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import com.microsoft.azure.documentdb.ChangeFeedOptions;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.PartitionKeyRange;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.ChangeFeedObserverFactory;
import com.microsoft.azure.documentdb.changefeedprocessor.services.DocumentCollectionInfo;

import java.util.concurrent.atomic.AtomicInteger;
import com.microsoft.azure.documentdb.changefeedprocessor.services.DocumentServices;


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
//    private IChangeFeedObserverFactory observerFactory;
//    private ExecutorService executorServicevice; not need as a property
    private Logger logger = Logger.getLogger(ChangeFeedEventHost.class.getName());

    public ChangeFeedEventHost( String hostName, DocumentCollectionInfo documentCollectionLocation, DocumentCollectionInfo auxCollectionLocation) throws DocumentClientException{
        this(hostName, documentCollectionLocation, auxCollectionLocation, new ChangeFeedOptions(), new ChangeFeedHostOptions());
    }

    public ChangeFeedEventHost(
            String hostName,
            DocumentCollectionInfo documentCollectionLocation,
            DocumentCollectionInfo auxCollectionLocation,
            ChangeFeedOptions changeFeedOptions,
            ChangeFeedHostOptions hostOptions) throws DocumentClientException {

    	
    	
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
      
        this.isShutdown = new AtomicInteger(0);
        
        try{
            this.documentServices = new DocumentServices(documentCollectionLocation);
        }
        catch(DocumentClientException ex){
            ex.printStackTrace();
        }

        this.checkpointSvcs = null;
        this.resourcePartitionSvcs = null;

        // Checking for ChangeFeed Page Size
        if (this.changeFeedOptions.getPageSize() == null || this.changeFeedOptions.getPageSize() == 0)
        {
            this.changeFeedOptions.setPageSize(this.DEFAULT_PAGE_SIZE);
        }
    }

    private DocumentCollectionInfo canonicalizeCollectionInfo(DocumentCollectionInfo collectionInfo) {
    	// [Done] CR: can we do some analog of Debug.Assert() for all private method input check?
    	assert collectionInfo != null;
        DocumentCollectionInfo result = collectionInfo;
        if (result.getConnectionPolicy().getUserAgentSuffix() == null ||
                result.getConnectionPolicy().getUserAgentSuffix().isEmpty())
        {
            result = new DocumentCollectionInfo(collectionInfo);
            result.getConnectionPolicy().setUserAgentSuffix(DefaultUserAgentSuffix);
        }

        return result;
    }
    
    public <T extends IChangeFeedObserver> void registerObserver(Class<T>  type) throws Exception, InterruptedException {	// CR: can we use generics? 
    	ChangeFeedObserverFactory<T> factory = new ChangeFeedObserverFactory<T>(type);
    	initializeIntegrations(factory);
    }
    
    public boolean unregisterObservers() {
    	logger.info("shutdown...");
    	boolean succ = true;
        try {
			this.partitionManager.stop(ChangeFeedObserverCloseReason.SHUTDOWN);
		} catch (InterruptedException | ExecutionException e) {
			succ = false;
		}
        this.documentServices.shudown();
        this.resourcePartitionSvcs.shutdown();
        logger.info("shutdown OK!");
        return succ;
    }
    
    
    private void initializeIntegrations() throws Exception, DocumentClientException, LeaseLostException, InterruptedException, ExecutionException {
        // Grab the options-supplied prefix if present otherwise leave it empty.
        
    	List<Callable<?>> initialTasks = new ArrayList<Callable<?>>();
    	
    	String optionsPrefix = this.options.getLeasePrefix();
        if (optionsPrefix == null) {
            optionsPrefix = "";
        }

        // Beyond this point all access to collection is done via this self link: if collection is removed, we won't access new one using same name by accident.
        this.leasePrefix = String.format("%s%s_%s_%s", optionsPrefix, this.collectionLocation.getUri().getHost(), documentServices.getDatabaseId(), documentServices.getCollectionId());

        this.leaseManager = new DocumentServiceLeaseManager(
                this.auxCollectionLocation,
                this.leasePrefix,
                this.options.getLeaseExpirationInterval(),
                this.options.getLeaseRenewInterval(),
                this.documentServices);

        initialTasks.add(new Callable<Boolean>() {

			@Override
			public Boolean call() throws Exception {
				leaseManager.initialize();
				return true;
			}
        	
        });

        this.checkpointSvcs = new CheckpointServices(this.leaseManager, this.options.getCheckpointFrequency());

        if (this.options.getDiscardExistingLeases()) {
            logger.warning(String.format("Host '%s': removing all leases, as requested by ChangeFeedHostOptions", this.hostName));
//            try {
//                this.leaseManager.deleteAll().call();
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
            
            initialTasks.add(this.leaseManager.deleteAll());
        }

        // Note: lease store is never stale as we use monitored collection Rid as id prefix for aux collection.
        // Collection was removed and re-created, the rid would change.
        // If it's not deleted, it's not stale. If it's deleted, it's not stale as it doesn't exist.
//        try {
//            this.leaseManager.createLeaseStoreIfNotExists().call();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
        
        initialTasks.add(this.leaseManager.createLeaseStoreIfNotExists());

        ConcurrentHashMap<String, PartitionKeyRange> ranges = this.documentServices.listPartitionRanges();

        this.leaseManager.createLeases(ranges);

        logger.info(String.format("Source collection: '%s', %d partition(s), %s document(s)", collectionLocation.getCollectionName(), ranges.size(), documentServices.getDocumentCount()));

        logger.info("Initializing partition manager");
        partitionManager = new PartitionManager<DocumentServiceLease>(this.hostName, this.leaseManager, this.options);
      
        	// [Done] CR: why is new ResourcePartitionServices inside try-catch?
        this.resourcePartitionSvcs = new ResourcePartitionServices(documentServices, checkpointSvcs, observerFactory, changeFeedOptions.getPageSize());

//        this.executorService.submit(partitionManager.subscribe(this)).get();    //Awaiting the task to be finished.  
//        this.executorService.submit(partitionManager.initialize()).get();       //Awaiting the task to be finished.
        
        initialTasks.add(partitionManager.subscribe(this));
        initialTasks.add(partitionManager.initialize());
        
        ExecutorService executorServicevice = Executors.newFixedThreadPool(1);
        logger.info("Initializing....");
        executorServicevice.invokeAll(initialTasks); //can add return to check the result of init
        executorServicevice.shutdown();
        logger.info("Initializaition done!");
        partitionManager.start();
    }

    @Override
    public Callable<Void> onPartitionAcquired(DocumentServiceLease documentServiceLease) {

        assert documentServiceLease != null && isNullOrEmpty(documentServiceLease.getOwner()) : "lease" ;
        Callable<Void> callable = new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                String[] parts = documentServiceLease.id.split(Pattern.quote("."));
                String partitionId = parts[parts.length-1];
                try {
                    resourcePartitionSvcs.create(partitionId);
                    // CR: we need to track new task for shutdown scenario.
                } catch (DocumentClientException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                // CR: eating exceptions.

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
                // CR: need to await for stop to finish
                return null;
            }
        };

        return callable;
    }

    public static boolean isNullOrEmpty(String s) {
        return s == null || s.length() == 0;
    }
    
    public <T extends IChangeFeedObserver> void registerObserver(Class<T>  type) throws Exception, InterruptedException {	// CR: can we use generics? 
    	ChangeFeedObserverFactory<T> factory = new ChangeFeedObserverFactory<T>(type);
    	initializeIntegrations(factory);
    }
    
    public boolean unregisterObservers() {
    	logger.info("shutdown...");
    	boolean succ = true;
        try {
			this.partitionManager.stop(ChangeFeedObserverCloseReason.SHUTDOWN);
		} catch (InterruptedException | ExecutionException e) {
			succ = false;
		}
        this.documentServices.shudown();
        this.resourcePartitionSvcs.shutdown();
        logger.info("shutdown OK!");
        return succ;
    }
}
