package com.microsoft.azure.documentdb.changefeedprocessor.services;

import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.changefeedprocessor.ChangeFeedObserverContext;
import com.microsoft.azure.documentdb.changefeedprocessor.IChangeFeedObserver;

import java.util.logging.Logger;

public class ChangeFeedJobBase implements Job {

    private final DocumentServices client;
    private final CheckpointServices checkpointSvcs;
    private final JobServices jobServices;
    private final IChangeFeedObserver observer;

    protected String partitionId;
    protected boolean shouldStop = false;

    /***
     *
     * @param client
     * @param checkpointSvcs
     * @param observer
     * @param jobServices
     */
    public ChangeFeedJobBase(DocumentServices client,
                             CheckpointServices checkpointSvcs,
                             IChangeFeedObserver observer,
                             JobServices jobServices
                             ) {
        this.client = client;
        this.checkpointSvcs = checkpointSvcs;
        this.observer = observer;
        this.jobServices = jobServices;
    }

    @Override
    public void start(Object initialData) throws DocumentClientException, InterruptedException {

        this.partitionId = (String) initialData;

        ChangeFeedObserverContext context = new ChangeFeedObserverContext();
        context.setPartitionKeyRangeId(partitionId);

        try {

        } catch(Exception e) {

        }
    }

    protected void checkpoint(Object data) throws DocumentClientException {
        String initialData = (String) (data == null ? "" : data);
        checkpointSvcs.setCheckpointData(partitionId, initialData);
    }

    @Override
    public void stop() {
        shouldStop = true;
        partitionId = null;
    }


}
