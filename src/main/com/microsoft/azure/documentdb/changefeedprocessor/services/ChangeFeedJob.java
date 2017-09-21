package com.microsoft.azure.documentdb.changefeedprocessor.services;

import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.FeedResponse;
import com.microsoft.azure.documentdb.changefeedprocessor.ChangeFeedObserverContext;
import com.microsoft.azure.documentdb.changefeedprocessor.IChangeFeedObserver;

import java.util.List;

public class ChangeFeedJob implements Job {

    private final DocumentServices _client;
    private final CheckpointServices _checkpointSvcs;
    private final String _partitionId;
    private final IChangeFeedObserver _observer;

    public ChangeFeedJob(String partitionId, DocumentServices client, CheckpointServices checkpointSvcs, IChangeFeedObserver observer) {
        this._client = client;
        this._checkpointSvcs = checkpointSvcs;
        this._partitionId = partitionId;
        this._observer = observer;


    }

    @Override
    public void start(Object initialData) {

        ChangeFeedObserverContext context = new ChangeFeedObserverContext();
        context.setPartitionKeyRangeId(_partitionId);
        FeedResponse<Document> query = null;
        this.checkpoint(initialData == null ? "":initialData);

        while(true) {

            try {
                query = _client.createDocumentChangeFeedQuery(_partitionId, (String) _checkpointSvcs.getCheckpointData(_partitionId));
                if (query != null) {
                    context.setFeedResponse(query);
                    List<Document> docs = query.getQueryIterable().fetchNextBlock();
                    if (docs != null) {
                        _observer.processChanges(context, docs);
                        this.checkpoint(query.getResponseContinuation());
                    }

                }
            } catch (Exception e) {
                e.printStackTrace();
            }

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    void checkpoint(Object data) {
        _checkpointSvcs.setCheckpointData(_partitionId, data);
    }

    @Override
    public void stop() {
        System.out.println("stopped");
    }
}
