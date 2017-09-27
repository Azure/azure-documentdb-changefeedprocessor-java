package com.microsoft.azure.documentdb.changefeedprocessor.services;

import com.microsoft.azure.documentdb.*;
import com.microsoft.azure.documentdb.changefeedprocessor.*;

import java.util.ArrayList;
import java.util.List;

public class DocumentServices {

    private final String url;
    private final String database;
    private final String collection;
    private final String masterKey;
    private final DocumentClient client;
    private final String collectionLink;

    public DocumentServices(DocumentCollectionInfo collectionLocation) {
        this.url = collectionLocation.getUri().toString();
        this.database = collectionLocation.getDatabaseName();
        this.collection = collectionLocation.getCollectionName();
        this.masterKey = collectionLocation.getMasterKey();
        this.client = new DocumentClient(url, masterKey, new ConnectionPolicy(), ConsistencyLevel.Session);
        this.collectionLink = String.format("/dbs/%s/colls/%s", database, collection);
    }

    public List<String> listPartitionRange() {

        String checkpointContinuation = null;
        FeedOptions options = new FeedOptions();

        List<PartitionKeyRange> partitionKeys = new ArrayList();
        List<String> partitionsId = new ArrayList();

        do {
            options.setRequestContinuation(checkpointContinuation);
            FeedResponse<PartitionKeyRange> range = client.readPartitionKeyRanges(collectionLink, options);
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

    public FeedResponse<Document> createDocumentChangeFeedQuery(String partitionId, String continuationToken, int pageSize) throws DocumentClientException {

        ChangeFeedOptions options = new ChangeFeedOptions();
        options.setPartitionKeyRangeId(partitionId);
        options.setPageSize(pageSize);

        if (continuationToken == null || continuationToken.isEmpty())
            options.setStartFromBeginning(true);
        else {
            options.setStartFromBeginning(false);
            options.setRequestContinuation(continuationToken);
        }

        FeedResponse<Document> query = client.queryDocumentChangeFeed(collectionLink, options);

        return query;
    }

    public DocumentChangeFeedClient createClient(String partitionId, String continuationToken) {
        return new DocumentChangeFeedClient(this, partitionId, continuationToken, 100);
    }

    public DocumentChangeFeedClient createClient(String partitionId, String continuationToken, int pageSize) {
        return new DocumentChangeFeedClient(this, partitionId, continuationToken, pageSize);
    }
}
