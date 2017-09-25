package com.microsoft.azure.documentdb.changefeedprocessor.services;

import com.microsoft.azure.documentdb.ChangeFeedOptions;
import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.FeedResponse;
import com.microsoft.azure.documentdb.changefeedprocessor.ChangeFeedObserverCloseReason;
import com.microsoft.azure.documentdb.changefeedprocessor.ChangeFeedObserverContext;
import com.microsoft.azure.documentdb.changefeedprocessor.IChangeFeedObserver;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.StatusCode;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.SubStatusCode;

import java.util.List;
import java.util.logging.Logger;

public class ChangeFeedJob implements Job {

    private final DocumentServices client;
    private final CheckpointServices checkpointSvcs;
    private final String partitionId;
    private final IChangeFeedObserver observer;
    private boolean stop = false;
    private ChangeFeedOptions feedOptions;
    private int pageSize;
    private final int DEFAULT_PAGE_SIZE = 100;
    private final int DEFAULT_THREAD_WAIT = 1000;

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
    }

    @Override
    public void start(Object initialData) throws DocumentClientException, InterruptedException {

        ChangeFeedObserverContext context = new ChangeFeedObserverContext();
        context.setPartitionKeyRangeId(partitionId);
        FeedResponse<Document> query = null;
        try {
            this.checkpoint(initialData);
        } catch (DocumentClientException e) {
            e.printStackTrace();
        }
        boolean HasMoreResults = false;
        ChangeFeedObserverCloseReason closeReason = null;

        while(!this.stop) {
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
                        closeReason = ChangeFeedObserverCloseReason.ResourceGone;
                        this.stop();
                    }else if(dce.getStatusCode() == StatusCode.CODE.Value()){
                        //TODO: handle partition split
                    }
                    else if (SubStatusCode.Splitting.Value() == subStatusCode)
                    {
                        logger.warning(String.format("Partition {0} is splitting. Will retry to read changes until split finishes. {1}", context.getPartitionKeyRangeId(), dce.getMessage()));
                    }
                    else
                    {
                        dce.printStackTrace();
                        throw dce;
                    }
                }
            }while (HasMoreResults && !this.stop );

            if (!this.stop)
            {
                Thread.sleep(this.DEFAULT_THREAD_WAIT);
            }

        }// while(!this.stop)
    }

    void checkpoint(Object data) throws DocumentClientException {
        String initialData = (String) (data == null ? "" : data);
        checkpointSvcs.setCheckpointData(partitionId, initialData);
    }

    @Override
    public void stop() {
        stop = true;
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
                //TODO:Log the error
            }
        }

        return -1;
    }
}
