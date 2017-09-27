package com.microsoft.azure.documentdb.changefeedprocessor.services;

import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.FeedResponse;
import com.microsoft.azure.documentdb.changefeedprocessor.ChangeFeedObserverCloseReason;
import com.microsoft.azure.documentdb.changefeedprocessor.ChangeFeedObserverContext;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.StatusCode;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.SubStatusCode;

import java.util.List;

public class DocumentChangeFeedClient {
    private final DocumentServices client;
    private final String partitionId;
    private final int pageSize;
    private String continuationToken;

    public DocumentChangeFeedClient(DocumentServices client, String partitionId, String continuationToken, int pageSize) {
        this.partitionId = partitionId;
        this.client = client;
        this.continuationToken = continuationToken;
        this.pageSize = pageSize;
    }

    public List<Document> read() throws DocumentChangeFeedException {
        try {
            List<Document> docs = null;

            FeedResponse<Document> query = client.createDocumentChangeFeedQuery(partitionId, continuationToken, pageSize);
            if (query != null) {
                //docs = query.getQueryIterable().toList();
                docs = query.getQueryIterable().fetchNextBlock();

//                List<Document> docs = query.getQueryIterable().fetchNextBlock();
//                boolean hasMoreResults = query.getQueryIterator().hasNext();
                //
                this.continuationToken = query.getResponseContinuation();
            }

            return docs;

        } catch (DocumentClientException dce) {
            int subStatusCode = getSubStatusCode(dce);
            if (dce.getStatusCode() == StatusCode.NOTFOUND.Value() &&
                    SubStatusCode.ReadSessionNotAvailable.Value() != subStatusCode){
                //closeReason = ChangeFeedObserverCloseReason.RESOURCE_GONE;

            }else if(dce.getStatusCode() == StatusCode.CODE.Value()){
                //TODO: handle partition split
            }
            else if (SubStatusCode.Splitting.Value() == subStatusCode)
            {
                //logger.warning(String.format("Partition {0} is splitting. Will retry to read changes until split finishes. {1}", context.getPartitionKeyRangeId(), dce.getMessage()));
            }
            else
            {
                dce.printStackTrace();
                //throw dce;
            }

            throw new DocumentChangeFeedException();
        }
    }

    public String getContinuationToken() {
        return continuationToken;
    }

    private int getSubStatusCode(DocumentClientException exception)
    {
        String SubStatusHeaderName = "x-ms-substatus";
        String valueSubStatus = exception.getResponseHeaders().get(SubStatusHeaderName);
        if (valueSubStatus != null && !valueSubStatus.isEmpty())
        {
            int subStatusCode = 0;
            try {
                return Integer.parseInt(valueSubStatus);
            }catch (Exception e){
                //TODO:Log the error
            }
        }

        return -1;
    }
}
