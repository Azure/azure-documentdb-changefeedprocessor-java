package com.microsoft.azure.documentdb.changefeedprocessor.services;

import com.microsoft.azure.documentdb.*;
import com.microsoft.azure.documentdb.DocumentClient;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.logging.Logger;

// CR: please put a comment what this class is used for, in particular, why we need same exact methods as in DocumentClient.
// CR: add comments for all other classes which are not trivial.
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

    // [Done] CR: can we do plain java (without lombok)? I installed lombox for Eclipse and Eclipse still generates build errors for getDatabaseID(), etc.
    //     This means lombok integration is sort of glitchy/not ready yet, customers may face this issue as well, so let's not use it.
    private String collectionSelfLink;
    private String collectionId;
    private String databaseSelfLink;
    private String databaseId;

    public String getCollectionSelfLink(){
        return collectionSelfLink;
    }

    public String getCollectionId(){
        return collectionId;
    }

    public String getDatabaseSelfLink(){
        return databaseSelfLink;
    }

    public String getDatabaseId(){
        return databaseId;
    }

    private Logger logger = Logger.getLogger(DocumentServices.class.getName());

    public DocumentServices(DocumentCollectionInfo collectionLocation) throws DocumentClientException {
        this.url = collectionLocation.getUri().toString();
        this.database = collectionLocation.getDatabaseName();
        this.collection = collectionLocation.getCollectionName();
        this.masterKey = collectionLocation.getMasterKey();
        this.client = new DocumentClient(url, masterKey, new ConnectionPolicy(), ConsistencyLevel.Session);
        this.collectionLink = String.format("/dbs/%s/colls/%s", database, collection);	// CR: can we use Paths.COLLECTIONS_PATH_SEGMENT, etc?
        this.databaseLink = String.format("/dbs/%s", database);							// CR: same here.
        Initialize();
    }

    /***
     * This initialization process does a request to the collection getting quota information usage.
     * this information will be used after in getDocumentCount method, (obs: the value returned can be different from the number of records at the collection)
     * It also update the collection selflink info.
     */
    private void Initialize() throws DocumentClientException {

        ResourceResponse<Database> databaseResponse = client.readDatabase(databaseLink, new RequestOptions());
        this.databaseId = databaseResponse.getResource().getId();
        this.databaseSelfLink = databaseResponse.getResource().getSelfLink();

        RequestOptions options = new RequestOptions();
        options.setPopulateQuotaInfo(true);

        ResourceResponse<DocumentCollection> response = null;
        response = client.readCollection(collectionLink, options);
        
        // CR: IMPORTANT: from now on, we should use collectionSelfLink (to avoid cases when collection/db is removed, then added with same name).
        // CR: remove collectionLink and databaseLink from fields, move them to variables in this method.
        
        if (response != null){
            collectionResponse = response;
            documentCollection = collectionResponse.getResource();
            collectionSelfLink = documentCollection.getSelfLink();
            collectionId = documentCollection.getId();
        }
    }

    public Hashtable<String, PartitionKeyRange> listPartitionRanges() {	// [Done] CR: listPartitionRanges?

        String checkpointContinuation = null;
        FeedOptions options = new FeedOptions();

        List<PartitionKeyRange> partitionKeys = new ArrayList<PartitionKeyRange>();
        Hashtable<String, PartitionKeyRange> partitionsId = new Hashtable<String, PartitionKeyRange>();	// CR: is there special Java naming convention that tells to use partitions in plural. Why not partitionIds?

        do {
            options.setRequestContinuation(checkpointContinuation);
            FeedResponse<PartitionKeyRange> range = client.readPartitionKeyRanges(collectionLink, options);  // CR: rename to ranges.
            try {
                partitionKeys.addAll(range.getQueryIterable().fetchNextBlock());
            } catch (DocumentClientException ex) {	// CR: why eat exceptions?
            }

            checkpointContinuation = range.getResponseContinuation(); //PartitionLSN	// Why partition LSN. This would not be LSN.
        } while (checkpointContinuation != null);

        for (PartitionKeyRange pkr : partitionKeys) {
            partitionsId.put(pkr.getId(), pkr);		// CR: do we really need extra array? Can do this on fetchNextBlock, right?
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

    public ResourceResponse<Document> createDocument(Object document, boolean disableIdGeneration) throws DocumentClientException {

        RequestOptions options = new RequestOptions();
        ResourceResponse<Document> response = client.createDocument(collectionSelfLink, document, options, disableIdGeneration);
        return response;
    }

    public ResourceResponse<Document> createDocument(String collectionLink, Object document, RequestOptions options, boolean disableIdGeneration) throws DocumentClientException {

        ResourceResponse<Document> response = client.createDocument(collectionLink, document, options, disableIdGeneration);
        return response;
    }

    // [Done] CR: change return type to int64. There are collections with # of docs greater than INT_MAX.
    public long getDocumentCount(){

        assert collectionResponse != null ;

        int result = -1;

        String resourceUsage = collectionResponse.getResponseHeaders().get("x-ms-resource-usage");
        if (resourceUsage != null)
        {
            String[] parts = resourceUsage.split(";");
            for(int i=0; i < parts.length; i++)
            {
                String[] name = parts[i].split("=");
                if (name.length > 1 && name[0].equals("documentsCount") && name[1] != null && !name[1].isEmpty())
                {
                    try {
                        result = Integer.parseInt(name[1]);
                    } catch (NumberFormatException ex) {
                        logger.warning(String.format("Failed to get document count from response, cant Integer.parseInt('%s')", name[1]));
                    }

                    break;
                }
            }
        }

        return result;
    }

    public ResourceResponse<DocumentCollection> readCollection(String uri, RequestOptions requestOptions) throws DocumentClientException {

        ResourceResponse<DocumentCollection> response = null;

        try {
            response = client.readCollection(uri, new RequestOptions());
        } catch (DocumentClientException ex){
            if (ex.getStatusCode() == 404 ) { //Collection Lease Not Found)
            // CR: make 404 a constant.
            // CR: why does the comment in line above say about lease collection?
            // CR: is there specific reason to special case not found and throw and in other cases return null?
                logger.info("Parameter createLeaseCollection is true! Creating lease collection");
                throw ex;
            }
        }
        return response;
    }

    public ResourceResponse<DocumentCollection> createCollection(String databaseLink, DocumentCollection leaseColl, RequestOptions requestOptions) throws DocumentClientException {

        return client.createCollection(databaseLink, leaseColl, requestOptions );
    }

    public void deleteDocument(String selfLink, RequestOptions requestOptions) throws DocumentClientException {

        client.deleteDocument(selfLink, requestOptions);	// CR: why not to do: return client.deleteDocument(...)? Why discard the value? 
    }

    public ResourceResponse<Document> readDocument(String uri, RequestOptions requestOptions) throws DocumentClientException {

        return client.readDocument(uri, requestOptions);
    }

    public ResourceResponse<Document> replaceDocument(String uri, Object obj, RequestOptions ifMatchOptions) throws DocumentClientException {

        return client.replaceDocument(uri, obj, ifMatchOptions );
    }

    public FeedResponse<Document> queryDocuments(String collectionLink, SqlQuerySpec querySpec, FeedOptions feedOptions) {

        return client.queryDocuments(collectionLink, querySpec, feedOptions);
    }
}
