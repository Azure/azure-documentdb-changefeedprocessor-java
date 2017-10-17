package com.microsoft.azure.documentdb.changefeedprocessor.services;

import com.microsoft.azure.documentdb.ChangeFeedOptions;
import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.FeedResponse;
import com.microsoft.azure.documentdb.changefeedprocessor.ChangeFeedObserverCloseReason;
import com.microsoft.azure.documentdb.changefeedprocessor.ChangeFeedObserverContext;
import com.microsoft.azure.documentdb.changefeedprocessor.IChangeFeedObserver;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.ChangeFeedThreadFactory;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.StatusCode;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.SubStatusCode;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

public class ChangeFeedJob implements Job {

    private final DocumentServices client;
    private final CheckpointServices checkpointSvcs;
    private final String partitionId;
    private final IChangeFeedObserver observer;
    //private boolean stop = false;
    private ChangeFeedOptions feedOptions;
    private int pageSize;
    private final int DEFAULT_PAGE_SIZE = 100;
    private final int DEFAULT_THREAD_WAIT = 1000;
    private ExecutorService exec;
    private final int CPUs = Runtime.getRuntime().availableProcessors();
    private final int NumThreadsPerCpu = 5;
    private static Logger logger = Logger.getLogger("com.microsoft.azure.documentdb.changefeedprocessor");

    /***
     *
     * @param partitionId
     * @param client
     * @param checkpointSvcs
     * @param observer
     * @param pageSize
     */
    public ChangeFeedJob(String partitionId,
                          DocumentServices client,
                          CheckpointServices checkpointSvcs,
                          IChangeFeedObserver observer,
                          int pageSize) {
        this.client = client;
        this.checkpointSvcs = checkpointSvcs;
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
     * @param observer
     */
    public ChangeFeedJob(String partitionId,
                          DocumentServices client,
                          CheckpointServices checkpointSvcs,
                          IChangeFeedObserver observer) {
        this.client = client;
        this.checkpointSvcs = checkpointSvcs;
        this.partitionId = partitionId;
        this.observer = observer;
        this.pageSize = DEFAULT_PAGE_SIZE;

        exec = CreateExecutorService(NumThreadsPerCpu, "partition_" + partitionId);
    }


    private ExecutorService CreateExecutorService(int numThreadPerCPU, String threadSuffixName ){

        if (numThreadPerCPU <= 0) throw new IllegalArgumentException("The parameter numThreadPerCPU must be greater them 0");
        if (threadSuffixName == null || threadSuffixName.isEmpty()) throw new IllegalArgumentException("The parameter threadSuffixName is null or empty");

        ChangeFeedThreadFactory threadFactory = new ChangeFeedThreadFactory(threadSuffixName);
        ExecutorService exec = Executors.newFixedThreadPool(numThreadPerCPU * CPUs, threadFactory);
        return exec;
    }

    @Override
    public void start(String initialData) throws DocumentClientException, InterruptedException {

        if (!exec.isShutdown() && !exec.isTerminated()) {
            exec.submit(() -> {
                try {
                    QueryChangeFeed(initialData);
                } catch (DocumentClientException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            });
        }
    }

    private void QueryChangeFeed(String initialData) throws DocumentClientException, InterruptedException{
        ChangeFeedObserverContext context = new ChangeFeedObserverContext();
        context.setPartitionKeyRangeId(partitionId);
        FeedResponse<Document> query = null;
        try {
            this.checkpoint(initialData);
        } catch (DocumentClientException e) {
            e.printStackTrace();
        }
        boolean HasMoreResults = false;

        while(!(exec.isTerminated() || exec.isShutdown())) {
            do {
                try {
                    query = client.createDocumentChangeFeedQuery(partitionId, (String) checkpointSvcs.getCheckpointData(partitionId), this.pageSize);
                    if (query != null) {
                        context.setFeedResponse(query);
                        List<Document> docs = query.getQueryIterable().fetchNextBlock();
                        HasMoreResults = query.getQueryIterator().hasNext();
                        if (docs != null) {
                            observer.processChanges(context, docs);
                            this.checkpoint(query.getResponseContinuation());
                        }
                    }
                } catch (DocumentClientException dce) {
                    int subStatusCode = getSubStatusCode(dce);
                    if (dce.getStatusCode() == StatusCode.NOTFOUND.Value() &&
                            SubStatusCode.ReadSessionNotAvailable.Value() != subStatusCode){
                        this.stop(ChangeFeedObserverCloseReason.RESOURCE_GONE);
                        observer.close(context,ChangeFeedObserverCloseReason.RESOURCE_GONE );
                    }else if(dce.getStatusCode() == StatusCode.CODE.Value()){
                        //TODO: handle partition split
                    }
                    else if (SubStatusCode.Splitting.Value() == subStatusCode)
                    {
                        logger.warning(String.format("Partition {0} is splitting. Will retry to read changes until split finishes. {1}", context.getPartitionKeyRangeId(), dce.getMessage()));
                    }
                    else
                    {
                        throw dce;
                    }
                }
            }while (HasMoreResults && !(exec.isTerminated() || exec.isShutdown()) );

            if (!(exec.isTerminated() || exec.isShutdown()))
            {
                exec.wait(this.DEFAULT_THREAD_WAIT);
            }

        }// while(!(exec.isTerminated() || exec.isShutdown()))
    }

    void checkpoint(String data) throws DocumentClientException {
        String initialData = (String) (data == null ? "" : data);
        checkpointSvcs.setCheckpointData(partitionId, initialData);
    }

    @Override
    public void stop(ChangeFeedObserverCloseReason CloseReason) {

        switch (CloseReason){
            case SHUTDOWN:
            case RESOURCE_GONE:
                logger.warning(String.format("CloseReason{0} Shutting down executor", CloseReason));
                exec.shutdown();
                break;
            default:
                logger.warning(String.format("CloseReason{0} Shutting down executor NOW", CloseReason));
                exec.shutdownNow();
                break;
        }

    }

    private int getSubStatusCode(DocumentClientException exception)
    {
        String SubStatusHeaderName = "x-ms-substatus";
        String valueSubStatus = exception.getResponseHeaders().get(SubStatusHeaderName);
        if (valueSubStatus != null && !valueSubStatus.isEmpty())
        {
            int subStatusCode = 0;
            try {
                return Integer.parseInt(valueSubStatus);
            }catch (Exception e){
                logger.severe(String.format("Not able to parse the error code {0} to int", valueSubStatus));
                //TODO:Log the error
            }
        }

        return -1;
    }
}
