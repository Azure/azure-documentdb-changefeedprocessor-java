/*
 * The MIT License (MIT)
 * Copyright (c) 2018 Microsoft Corporation
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
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
    private ExecutorService executorService;
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


        this.executorService = Executors.newFixedThreadPool(1);
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
     *
     * @param type the type
     */
    public void registerObserver(Class type) throws Exception
    {
        logger.info(String.format("Registering Observer of type %s", type));
        
        this.executorService.execute(()->{
            try {
                start();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    public void registerObserverFactory(IChangeFeedObserverFactory factory) {
        logger.info(String.format("Registering Observer of type %s", factory.getClass()));
        this.observerFactory = factory;
        
        this.executorService.execute(()->{
            try {
                start();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    private void start() throws Exception{
        logger.info(String.format("Starting..."));

        initializeIntegrations();

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
                return null;
            }
        };

        return callable;

    }

    public ExecutorService getExecutorService(){
        return executorService;
    }
}
