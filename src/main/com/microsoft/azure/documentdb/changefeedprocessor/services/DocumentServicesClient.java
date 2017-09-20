package com.microsoft.azure.documentdb.changefeedprocessor.services;

import com.microsoft.azure.documentdb.*;

import java.util.ArrayList;
import java.util.List;

public class DocumentServicesClient {

    private final DocumentClient _client;
    private final String _collectionLink;
    private final ChangeFeedOptions _options;

    public DocumentServicesClient(String url, String database, String collection, String masterKey) {
        this._client = new DocumentClient(url, masterKey, new ConnectionPolicy(), ConsistencyLevel.Session);
        this._collectionLink = String.format("/dbs/%s/colls/%s", database, collection);
        this._options = new ChangeFeedOptions ();

        // setPartitionKeyRangeId
        // options.setRequestContinuation(checkpointContinuation);
    }

    public Object listPartitionRange() {

        String checkpointContinuation = null;
        FeedOptions options = new FeedOptions();

        List<PartitionKeyRange> partitionKeys = new ArrayList<>();

        do {
            options.setRequestContinuation(checkpointContinuation);
            FeedResponse<PartitionKeyRange> range = _client.readPartitionKeyRanges(_collectionLink, options);
            try {
                partitionKeys.addAll(range.getQueryIterable().fetchNextBlock());
            }catch (DocumentClientException ex){}

            checkpointContinuation = range.getResponseContinuation(); //PartitionLSN
        } while (checkpointContinuation != null);

        return partitionKeys;
    }

    public FeedResponse<Document> createDocumentChangeFeedQuery(String partitionId) throws Exception {
        FeedResponse<Document> query = _client.queryDocumentChangeFeed(_collectionLink, _options);;

        if( query != null ) {
            _options.setRequestContinuation(query.getResponseContinuation());
        }

        return query;
    }
}
