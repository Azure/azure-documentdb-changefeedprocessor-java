package com.microsoft.azure.documentdb.changefeedprocessor.services;

import com.microsoft.azure.documentdb.*;
import com.microsoft.azure.documentdb.changefeedprocessor.*;

import java.util.ArrayList;
import java.util.List;

public class DocumentServices {

    private final String _url;
    private final String _database;
    private final String _collection;
    private final String _masterKey;
    private final DocumentClient _client;
    private final String _collectionLink;
//    private final ChangeFeedOptions _options;

    public DocumentServices(DocumentCollectionInfo collectionLocation) {
        this._url = collectionLocation.getUri().toString();
        this._database = collectionLocation.getDatabaseName();
        this._collection = collectionLocation.getCollectionName();
        this._masterKey = collectionLocation.getMasterKey();
        this._client = new DocumentClient(_url, _masterKey, new ConnectionPolicy(), ConsistencyLevel.Session);
        this._collectionLink = String.format("/dbs/%s/colls/%s", _database, _collection);
//        this._options = new ChangeFeedOptions ();
    }

    public List<String> listPartitionRange() {

        String checkpointContinuation = null;
        FeedOptions options = new FeedOptions();

        List<PartitionKeyRange> partitionKeys = new ArrayList();
        List<String> partitionsId = new ArrayList();

        do {
            options.setRequestContinuation(checkpointContinuation);
            FeedResponse<PartitionKeyRange> range = _client.readPartitionKeyRanges(_collectionLink, options);
            try {
                partitionKeys.addAll(range.getQueryIterable().fetchNextBlock());
            }catch (DocumentClientException ex){}

            checkpointContinuation = range.getResponseContinuation(); //PartitionLSN
        } while (checkpointContinuation != null);


        for(PartitionKeyRange pkr : partitionKeys) {
            partitionsId.add(pkr.getId());
        }

        return partitionsId;
    }

    public FeedResponse<Document> createDocumentChangeFeedQuery(String partitionId, String continuationToken) throws Exception {

        ChangeFeedOptions options = new ChangeFeedOptions();
        options.setPartitionKeyRangeId(partitionId);

        if (continuationToken == null || continuationToken.isEmpty())
            options.setStartFromBeginning(true);
        else {
            options.setStartFromBeginning(false);
            options.setRequestContinuation(continuationToken);
        }

        FeedResponse<Document> query = _client.queryDocumentChangeFeed(_collectionLink, options);

        return query;
    }
}
