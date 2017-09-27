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

        String continuationToken = checkpointSvcs.getCheckpoint(partitionId);

        DocumentChangeFeedClient client = documentServices.createClient(partitionId, continuationToken);

        while(!this.stop) {

            boolean hasMoreResults = true;
            List<Document> docs = null;


            try {
                docs = client.read();
            } catch(DocumentChangeFeedException e) {

            }

            hasMoreResults = (docs != null);

            if (hasMoreResults || this.stop)
                continue;

            Thread.sleep(this.DEFAULT_THREAD_WAIT);
        }
    }

    void checkpoint(Object data) throws DocumentClientException {
        String initialData = (String) (data == null ? "" : data);
        checkpointSvcs.setCheckpointData(partitionId, initialData);
    }

    @Override
    public void stop() {
        stop = true;
    }

}
