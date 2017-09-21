package com.microsoft.azure.documentdb.changefeedprocessor.services;

import com.microsoft.azure.documentdb.ChangeFeedOptions;
import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.FeedResponse;
import com.microsoft.azure.documentdb.changefeedprocessor.ChangeFeedObserverContext;
import com.microsoft.azure.documentdb.changefeedprocessor.IChangeFeedObserver;

import java.util.List;

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
    public void start(Object initialData) {

        ChangeFeedObserverContext context = new ChangeFeedObserverContext();
        context.setPartitionKeyRangeId(partitionId);
        FeedResponse<Document> query = null;
        this.checkpoint(initialData);
        boolean HasMoreResults = false;

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
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }while (HasMoreResults && !this.stop );

            if (!this.stop)
            {
                try {
                    Thread.sleep(this.DEFAULT_THREAD_WAIT);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

        }// while(!this.stop)
    }

    void checkpoint(Object data) {

        String initialData = (String) (data == null ? "" : data);
        checkpointSvcs.setCheckpointData(partitionId, initialData);
    }

    @Override
    public void stop() {
        stop = true;
    }
}
