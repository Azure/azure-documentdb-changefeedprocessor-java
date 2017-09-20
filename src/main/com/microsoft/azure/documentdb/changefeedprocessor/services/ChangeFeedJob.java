package com.microsoft.azure.documentdb.changefeedprocessor.services;

public class ChangeFeedJob implements Job {

    private final DocumentServices _client;
    private final CheckpointServices _checkpointSvcs;
    private final String _partitionId;

    public ChangeFeedJob(String partitionId, DocumentServices client, CheckpointServices checkpointSvcs) {
        this._client = client;
        this._checkpointSvcs = checkpointSvcs;
        this._partitionId = partitionId;
    }

    @Override
    public void start(Object initialData) {

//        service = new DocumentServices(docInfo);
//        DocumentServicesClient client =  service.createClient();
        // _client.createDocumentChangeFeedQuery();

        while(true) {
            System.out.println("running");
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
