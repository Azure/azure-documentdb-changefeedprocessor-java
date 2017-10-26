package com.microsoft.azure.documentdb.changefeedprocessor.services;

import com.microsoft.azure.documentdb.*;
import com.microsoft.azure.documentdb.changefeedprocessor.*;
import lombok.Getter;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.logging.Logger;

public class DocumentServices {

    private final String url;
    private final String database;
    private final String collection;
    private final String masterKey;
    private final DocumentClient client;
    private final String collectionLink;
    private final String databaseLink;
    private DocumentCollection documentCollection;
    private ResourceResponse<DocumentCollection> collectionResponse;

    @Getter
    private String collectionSelfLink;
    @Getter
    private String collectionID;
    @Getter
    private String databaseSelfLink;
    @Getter
    private String databaseID;

    private Logger logger = Logger.getLogger(DocumentServices.class.getName());

    public DocumentServices(DocumentCollectionInfo collectionLocation) {
        this.url = collectionLocation.getUri().toString();
        this.database = collectionLocation.getDatabaseName();
        this.collection = collectionLocation.getCollectionName();
        this.masterKey = collectionLocation.getMasterKey();
        this.client = new DocumentClient(url, masterKey, new ConnectionPolicy(), ConsistencyLevel.Session);
        this.collectionLink = String.format("/dbs/%s/colls/%s", database, collection);
        this.databaseLink = String.format("/dbs/%s", database);
        Initialize();

    }

    /***
     * This initialization process does a request to the collection getting quota information usage.
     * this information will be used after in getDocumentCount method, (obs: the valure returned can be different from the number os records at the collection)
     * It also update the collection selflink info.
     */
    private void Initialize() {

        try {
            ResourceResponse databaseResponse = client.readDatabase(databaseLink, new RequestOptions());
            databaseID = databaseResponse.getResource().getId();
            databaseSelfLink = databaseResponse.getResource().getSelfLink();
        } catch (DocumentClientException e) {
            e.printStackTrace();
        }

        RequestOptions options = new RequestOptions();
        options.setPopulateQuotaInfo(true);

        ResourceResponse<DocumentCollection> response = null;
        try {
            response  = client.readCollection(collectionLink, options);
        } catch (DocumentClientException e) {
            e.printStackTrace();
        }

        if (response != null){
            collectionResponse = response;
            documentCollection = collectionResponse.getResource();
            collectionSelfLink = documentCollection.getSelfLink();
            collectionID = documentCollection.getId();
        }

    }

    public Hashtable<String, PartitionKeyRange> listPartitionRange() {

        String checkpointContinuation = null;
        FeedOptions options = new FeedOptions();

        List<PartitionKeyRange> partitionKeys = new ArrayList();
        Hashtable<String, PartitionKeyRange> partitionsId = new Hashtable();

        do {
            options.setRequestContinuation(checkpointContinuation);
            FeedResponse<PartitionKeyRange> range = client.readPartitionKeyRanges(collectionLink, options);
            try {
                partitionKeys.addAll(range.getQueryIterable().fetchNextBlock());
            }catch (DocumentClientException ex){}

            checkpointContinuation = range.getResponseContinuation(); //PartitionLSN
        } while (checkpointContinuation != null);


        for(PartitionKeyRange pkr : partitionKeys) {
            partitionsId.put(pkr.getId(),pkr );
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

    public ResourceResponse createDocument(Object document, boolean disableIdGeneration) throws DocumentClientException {

        RequestOptions options = new RequestOptions();
        ResourceResponse response = client.createDocument(collectionSelfLink, document, options, disableIdGeneration );
        return response;
    }

    public int getDocumentCount(){

        if (collectionResponse == null)
            return -1;

        int result = -1;

        String resourceUsage = collectionResponse.getResponseHeaders().get("x-ms-resource-usage");
        if (resourceUsage != null)
        {
            String[] parts = resourceUsage.split(";");
            for(int i=0; i < parts.length; i++)
            {
                String[] name = parts[i].split("=");
                if (name.length > 1 && name[0].equals("documentsCount") && name[1] != null &&
                        !name[1].isEmpty())
                {
                    try {

                        result = Integer.parseInt(name[1]);
                    }catch (NumberFormatException ex)
                    {
                        logger.warning(String.format("Failed to get document count from response, cant Integer.parseInt('%s')", name[1]));
                    }

                    break;
                }
            }
        }

        return result;
    }
}
