package com.microsoft.azure.documentdb.changefeedprocessor.services;

import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.changefeedprocessor.IChangeFeedObserver;

import java.util.logging.Logger;

public class ChangeFeedJobBase implements Job {

    protected static final int DEFAULT_THREAD_WAIT = 1000;

    private final DocumentServices documentServices;
    private final CheckpointServices checkpointSvcs;
    private final IChangeFeedObserver observer;
    private boolean stop = false;
    private String partitionId;
    private boolean isRunning;

    /***
     *
     * @param checkpointSvcs
     * @param observer
     */
    public ChangeFeedJobBase(DocumentServices documentServices,
                             CheckpointServices checkpointSvcs,
                             IChangeFeedObserver observer) {
        this.documentServices = documentServices;
        this.checkpointSvcs = checkpointSvcs;
        this.observer = observer;
        this.isRunning = false;
    }

    @Override
    public void start(Object initialData) {

        this.isRunning = true;
        this.partitionId = (String)initialData;

        try {
            String continuationToken = checkpointSvcs.getContinuationToken(partitionId);
            DocumentChangeFeedClient client = documentServices.createClient(partitionId, continuationToken);

            runLoop(observer, client, partitionId, continuationToken);

        } catch(InterruptedException e) {
            // if this.stop == true...
            // else...

        } catch(Exception e) {

        }

        this.isRunning = false;
    }

    protected void runLoop(IChangeFeedObserver observer, DocumentChangeFeedClient client, String partitionId, String continuationToken) throws Exception {
    }

    protected void sleep(int totalTime) throws InterruptedException {
        int interval = 10;
        for(int totalSleep=0; totalSleep < totalTime; totalSleep += interval ) {
            checkStopInterruption();
            Thread.sleep( interval );
        }
    }

    protected void checkpoint(String continuationToken) {
        try {
            String initialData = continuationToken == null ? "" : continuationToken;
            checkpointSvcs.setContinuationToken(partitionId, initialData);
        } catch(DocumentClientException e) {
            // silent ignore... ?
        }
    }

    private void checkStopInterruption() throws InterruptedException {
        synchronized(this) {
            if( this.stop )
                throw new InterruptedException();
        }
    }

    @Override
    public void stop() {
        synchronized (this) {
            stop = true;
        }
    }

    public boolean checkIsRunning() {
        return this.isRunning;
    }
}
