package com.microsoft.azure.documentdb.changefeedprocessor.services;

import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.FeedResponse;
import com.microsoft.azure.documentdb.changefeedprocessor.ChangeFeedObserverContext;
import com.microsoft.azure.documentdb.changefeedprocessor.IChangeFeedObserver;

import java.util.List;
import java.util.logging.Logger;

public class ChangeFeedJob3 extends ChangeFeedJobBase {

    private static Logger logger = Logger.getLogger("com.microsoft.azure.documentdb.changefeedprocessor");

    /***
     *
     * @param checkpointSvcs
     * @param observer
     */
    public ChangeFeedJob3(DocumentServices documentServices,
                          CheckpointServices checkpointSvcs,
                          IChangeFeedObserver observer) {
        super(documentServices, checkpointSvcs, observer);
    }


    @Override
    protected void runLoop(IChangeFeedObserver observer, DocumentChangeFeedClient client, String partitionId, String continuationToken) throws Exception {

        boolean hasMoreResults = true;

        while(true) {
            List<Document> docs = null;

            try {
                docs = client.read();

                processChanges(observer, partitionId, docs);

                hasMoreResults = (docs != null);

            } catch(DocumentChangeFeedException e) {
                hasMoreResults = false; // force false for now
            }

            if (hasMoreResults)
                continue;

            sleep(DEFAULT_THREAD_WAIT);
        }
    }

    void processChanges(IChangeFeedObserver observer, String partitionId, List<Document> docs) {
        // return null for feedresponse (temporarily)
        // suggestion: remove from API
        FeedResponse<Document> WRONG_BUT_WORKS = null;
        ChangeFeedObserverContext context = new ChangeFeedObserverContext();
        context.setPartitionKeyRangeId(partitionId);
        context.setFeedResponse(WRONG_BUT_WORKS);
        observer.processChanges(context, docs);
    }

}
