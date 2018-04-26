package com.microsoft.azure.documentdb.changefeedprocessor;

import com.microsoft.azure.documentdb.ChangeFeedOptions;
import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.FeedResponse;
import com.microsoft.azure.documentdb.PartitionKeyRange;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.ChangeFeedThreadFactory;
import com.microsoft.azure.documentdb.changefeedprocessor.services.DocumentServices;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;

//This class contains the job definition(QueryChangeFeed() method) that reads data from Cosmos DB Changefeed.
public class ChangeFeedJob implements Job {

    private final DocumentServices client;
    private final CheckpointServices checkpointSvcs;
    private ILeaseManager<DocumentServiceLease> documentLeaseMgr;

    private final String partitionId;
    private final IChangeFeedObserver observer;
    private int pageSize;
    private final int DEFAULT_PAGE_SIZE = 100;
    private final int DEFAULT_THREAD_WAIT = 1000;
    private ExecutorService exec;
    private final int CPUs = Runtime.getRuntime().availableProcessors();
    private final int NumThreadsPerCpu = 5;
    private static Logger logger = Logger.getLogger(ChangeFeedJob.class.getName());

    public DocumentServices getClient(){
        return client;
    }

    public CheckpointServices getCheckpointSvcs(){
        return checkpointSvcs;
    }
    /***
     *
     * @param partitionId
     * @param client
     * @param checkpointSvcs
     * @param docLeaseMgr
     * @param observer
     * @param pageSize
     */
    public ChangeFeedJob(String partitionId,
                          DocumentServices client,
                          CheckpointServices checkpointSvcs,
                          DocumentServiceLeaseManager docLeaseMgr,
                          IChangeFeedObserver observer,
                          int pageSize) {
        this.client = client;
        this.checkpointSvcs = checkpointSvcs;
        this.documentLeaseMgr = docLeaseMgr;
        this.partitionId = partitionId;
        this.observer = observer;
        this.pageSize = pageSize;

        exec = CreateExecutorService(NumThreadsPerCpu, "partition_" + partitionId);
    }

    /**
     *
     * @param partitionId
     * @param client
     * @param checkpointSvcs
     * @param docLeaseMgr
     * @param observer
     */
    public ChangeFeedJob(String partitionId,
                          DocumentServices client,
                          CheckpointServices checkpointSvcs,
                          DocumentServiceLeaseManager docLeaseMgr,
                          IChangeFeedObserver observer) {
        this.client = client;
        this.checkpointSvcs = checkpointSvcs;
        this.documentLeaseMgr = docLeaseMgr;
        this.partitionId = partitionId;
        this.observer = observer;
        this.pageSize = DEFAULT_PAGE_SIZE;

        exec = CreateExecutorService(NumThreadsPerCpu, "partition_" + partitionId);
    }


    private ExecutorService CreateExecutorService(int numThreadPerCPU, String threadSuffixName ){

        logger.info(String.format("Creating ExecutorService CPUs: %d, numThreadPerCPU: %d, threadSuffixName: %s",CPUs, numThreadPerCPU, threadSuffixName));
        if (numThreadPerCPU <= 0) throw new IllegalArgumentException("The parameter numThreadPerCPU must be greater them 0");
        if (threadSuffixName == null || threadSuffixName.isEmpty()) throw new IllegalArgumentException("The parameter threadSuffixName is null or empty");

        ChangeFeedThreadFactory threadFactory = new ChangeFeedThreadFactory(threadSuffixName);
        ExecutorService exec = Executors.newFixedThreadPool(numThreadPerCPU * CPUs, threadFactory);

        return exec;
    }

    @Override
    public void start(String initialData, DocumentServiceLease dsl) throws DocumentClientException, InterruptedException {
        logger.info(String.format("Starting ChangeFeedJob "));
        if (!exec.isShutdown() && !exec.isTerminated()) {

            exec.execute(() -> {
                try {
                    QueryChangeFeed(this.documentLeaseMgr, initialData, dsl);
                } catch (DocumentClientException e) {
                    logger.severe(e.getMessage());
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    logger.severe(e.getMessage());
                    e.printStackTrace();
                } catch (Exception e) {
                	logger.severe(e.getMessage());
                }
            });
        }
    }

    private void QueryChangeFeed(ILeaseManager<DocumentServiceLease> dslm, String initialData, DocumentServiceLease dsl) throws Exception{
        logger.info(String.format("PartitionID: %s - QueryChangeFeed - Initiate", partitionId));
        ChangeFeedObserverContext context = new ChangeFeedObserverContext();
        context.setPartitionKeyRangeId(partitionId);
        FeedResponse<Document> query = null;

        boolean hasMoreResults = false;	
        String continuationToken = checkpointSvcs.getCheckpointData(partitionId);

        while (!(exec.isTerminated() || exec.isShutdown())) {
            do {
            	DocumentClientException dcex = null;
            	
                try {
                    logger.info(String.format("client.createDocumentChangeFeedQuery(%s, %s, %d)",partitionId, checkpointSvcs.getCheckpointData(partitionId), this.pageSize));
                    //Calling the Changefeed API and getting documents
                    query = client.createDocumentChangeFeedQuery(partitionId, checkpointSvcs.getCheckpointData(partitionId), this.pageSize);
                    if (query != null) {
                        logger.info(String.format("Query is not null query.getActivityId: %s ", query.getActivityId()));
                        context.setFeedResponse(query);
                        List<Document> docs = query.getQueryIterable().toList();
                        continuationToken = query.getResponseContinuation();
                        hasMoreResults = query.getQueryIterator().hasNext();
                        if (docs != null) {
                            logger.info(String.format("Docs Loaded #%d - HasMoreResults: %s",docs.size(), hasMoreResults));
                            observer.processChanges(context, docs);	//Calling the client's processChanges() implementation and sending over the documents
                            this.checkpoint(query.getResponseContinuation());
                        } else {
                            logger.info(String.format("Docs is null & HasMoreResults = %s", hasMoreResults));
                        }
                    }
                } catch (DocumentClientException dce) { 
                	dcex = dce;
                } catch (Exception ex) {
                	//Closing the observer since we ran into an unknown issue
                    logger.severe(String.format("Other exception not handled has happened: %s ",ex.getMessage()));
                    observer.close(context, ChangeFeedObserverCloseReason.UNKNOWN);
                    
                }
                if(dcex != null) {
                	int subStatusCode = getSubStatusCode(dcex);
                    if ((dcex.getStatusCode() == StatusCode.NOTFOUND.Value() &&
                            SubStatusCode.ReadSessionNotAvailable.Value() != subStatusCode)) {
                    	// Most likely, the database or collection was removed while we were enumerating.
                        // Shut down. The user will need to start over.
                        // Note: this has to be a new task, can't await for shutdown here, as shudown awaits for all worker tasks.
                    	logger.warning(String.format("Partition {0}: resource gone (subStatus={1}). Aborting.", context.getPartitionKeyRangeId(), getSubStatusCode(dcex)));
                        this.stop(ChangeFeedObserverCloseReason.RESOURCE_GONE);
                        break;
                        // CR: important: where is handle split? SubStatusCode.PartitionKeyRangeGone is not used anywhere in the project!
                    } else if (StatusCode.GONE.Value() == dcex.getStatusCode()) {
	                	if(SubStatusCode.PartitionKeyRangeGone.Value() == subStatusCode) {
	                		boolean isSuccess = handleSplits(dslm, context.getPartitionKeyRangeId(), continuationToken, dsl.id); 
	                		if (!isSuccess) {
	                            logger.warning(String.format("Partition {0}: HandleSplit failed! Aborting.", context.getPartitionKeyRangeId()));
	                            this.stop(ChangeFeedObserverCloseReason.RESOURCE_GONE);
	                            break;
	                        }
	
	                        // Throw LeaseLostException so that we take the lease down.
	                        throw new LeaseLostException(dsl, dcex, true);
	                	} else if (SubStatusCode.Splitting.Value() == subStatusCode) {
	                        logger.warning(String.format("Partition %s is splitting. Will retry to read changes until split finishes. %s", context.getPartitionKeyRangeId(), dcex.getMessage()));
	                	} else {
	                		throw dcex;
	                	}
	                } else if (dcex.getStatusCode() == StatusCode.TOO_MANY_REQUESTS.Value() || dcex.getStatusCode() == StatusCode.SERVICE_UNAVAILABLE.Value()) {
	                	logger.warning(String.format("Partition {0}: retriable exception : {1}", context.getPartitionKeyRangeId(), dcex.getMessage()));
	                	exec.awaitTermination(this.DEFAULT_THREAD_WAIT, TimeUnit.MILLISECONDS);
	                } else if (dcex.getMessage().contains("Reduce page size and try again.")) {
	                	
	                } else {
	                        throw dcex;
	                }
                }

            } while (hasMoreResults && !(exec.isTerminated() || exec.isShutdown()));

            if (!(exec.isTerminated() || exec.isShutdown())) {
                try {
                    exec.awaitTermination(this.DEFAULT_THREAD_WAIT, TimeUnit.MILLISECONDS);
                    logger.info(String.format("There are no changes for Partition: %s - Waiting for %d milliseconds before perform another query.", this.partitionId, this.DEFAULT_THREAD_WAIT));
                } catch (InterruptedException e) {
                    logger.warning(String.format(" InterruptedException trying to wait the thread: %s", e.getMessage()));
                }
            }
        }
    }

    void checkpoint(String data) throws DocumentClientException {
        logger.info(String.format("Chekpoint - PartitionID: %s, InitialData %s",this.partitionId, data));
        String initialData = "";

        if (data != null)
            initialData = data;

        checkpointSvcs.setCheckpointData(partitionId, initialData);
    }

    @Override
    public void stop(ChangeFeedObserverCloseReason CloseReason) {

        switch (CloseReason){
            case SHUTDOWN:
            case RESOURCE_GONE:
                logger.warning(String.format("CloseReason %s Shutting down executor", CloseReason));
                exec.shutdown();//maybe need to add a timeout
                break;
            default:
                logger.warning(String.format("CloseReason %s Shutting down executor NOW", CloseReason));
                exec.shutdownNow();
                break;
        }
    }

    private int getSubStatusCode(DocumentClientException exception)
    {
        assert exception != null ;
        String SubStatusHeaderName = "x-ms-substatus";
        String valueSubStatus = exception.getResponseHeaders().get(SubStatusHeaderName);
        if (valueSubStatus != null && !valueSubStatus.isEmpty())
        {
            //int subStatusCode = 0;
            try {
                return Integer.parseInt(valueSubStatus);
            }catch (Exception e){
                logger.severe(String.format("Not able to parse the error code %s to int", valueSubStatus));
            }
        }

        return -1;
    }
    
    private /*Callable<Boolean>*/ boolean handleSplits(ILeaseManager<DocumentServiceLease> dslm, String partitionKeyRangeId, String continuationToken, String leaseId) throws InterruptedException, ExecutionException, DocumentClientException {
    /*	Callable<Boolean> callable = new Callable<Boolean>() {
    		@Override
    		public Boolean call() throws InterruptedException, ExecutionException, DocumentClientException { */
    			assert partitionKeyRangeId != null && partitionKeyRangeId.isEmpty();
    	    	assert leaseId != null && leaseId.isEmpty();
    	    	logger.info(String.format("Partition {0} is gone due to split, continuation '{1}'",partitionKeyRangeId, continuationToken));
    	    	ExecutorService exec = Executors.newFixedThreadPool(1);
    	    	List<PartitionKeyRange> allRange = exec.submit(CollectionHelper.enumPartitionKeyRangesAsync(ChangeFeedJob.this.client.getDocumentClient(), ChangeFeedJob.this.client.getCollectionSelfLink())).get();
    	    	List<PartitionKeyRange> childRanges = new ArrayList<PartitionKeyRange>(allRange.stream().filter(range -> range.getParents().contains(partitionKeyRangeId)).collect(Collectors.toList()));
    	    	
                if (childRanges.size() < 2) {
                    logger.warning(String.format("Partition {0} had split but we failed to find at least 2 child paritions.", partitionKeyRangeId));
                    return false;
                }

                List<Callable<Boolean>> callables = new ArrayList<Callable<Boolean>>();
                for (PartitionKeyRange childRange : childRanges) {
                    callables.add(dslm.createLeaseIfNotExists(childRange.getId(), continuationToken));
                    logger.info(String.format("Creating lease for partition '{0}' as child of partition '{1}', continuation '{2}'", childRange.getId(), partitionKeyRangeId, continuationToken));
                }

                exec = Executors.newCachedThreadPool();
                exec.invokeAll(callables);
                DocumentServiceLease dsl = new DocumentServiceLease();
                dsl.setId(leaseId);
                dslm.delete(dsl);

                logger.info(String.format("Deleted lease for gone (splitted) partition '{0}' continuation '{1}'", partitionKeyRangeId, continuationToken));

                // Note: the rest is up to lease taker, that after waking up would consume these new leases.
                return true;
                }
   /* 	};
    	
    	return callable; 
    } */
}
