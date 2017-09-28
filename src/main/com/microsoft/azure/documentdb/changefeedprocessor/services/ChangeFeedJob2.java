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

public class ChangeFeedJob2 implements Job {

    private final CheckpointServices checkpointSvcs;
    private final IChangeFeedObserver observer;
    private final DocumentServices documentServices;
    private boolean stop = false;
    private String partitionId;
    private final int DEFAULT_THREAD_WAIT = 1000;

    private static Logger logger = Logger.getLogger("com.microsoft.azure.documentdb.changefeedprocessor");


    /***
     *
     * @param checkpointSvcs
     * @param observer
     */
    public ChangeFeedJob2(DocumentServices documentServices,
                          CheckpointServices checkpointSvcs,
                          IChangeFeedObserver observer) {
        this.documentServices = documentServices;
        this.checkpointSvcs = checkpointSvcs;
        this.observer = observer;
    }


    @Override
    public void start(Object initialData) throws DocumentClientException, InterruptedException {

        this.partitionId = (String)initialData;

        String continuationToken = ""; // checkpointSvcs.getCheckpoint(partitionId);

        DocumentChangeFeedClient client = documentServices.createClient(partitionId, continuationToken);

        while(!this.stop) {

            boolean hasMoreResults = true;
            List<Document> docs = null;


            try {
                docs = client.read();

                if(docs == null)
                    continue;

                processChanges(docs);

                hasMoreResults = true;

            } catch(DocumentChangeFeedException e) {
                hasMoreResults = false; // force false for now
            }

            if (this.stop)
                break;

            if (hasMoreResults)
                continue;

            sleep(DEFAULT_THREAD_WAIT);
        }

        this.partitionId = null;
    }

    void sleep(int totalTime) throws InterruptedException {
        int interval = 10;
        for(int totalSleep=0; totalSleep < totalTime; totalSleep += interval ) {
            // check if the stop is signaled
            if( this.stop )
                break;
            Thread.sleep( interval );
        }

    }

    void processChanges(List<Document> docs) {
        // return null for feedresponse (temporarily)
        // suggestion: remove from API
        FeedResponse<Document> WRONG_BUT_WORKS = null;
        ChangeFeedObserverContext context = new ChangeFeedObserverContext();
        context.setPartitionKeyRangeId(partitionId);
        context.setFeedResponse(WRONG_BUT_WORKS);
        observer.processChanges(context, docs);
    }

    void checkpoint(Object data) throws DocumentClientException {
        String initialData = (String) (data == null ? "" : data);
        checkpointSvcs.setContinuationToken(partitionId, initialData);
    }

    @Override
    public void stop() {
        stop = true;
    }

    public boolean checkIsRunning() {
        return this.partitionId != null;
    }
}
