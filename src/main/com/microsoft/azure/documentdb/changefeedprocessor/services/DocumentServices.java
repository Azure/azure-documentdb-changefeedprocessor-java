package com.microsoft.azure.documentdb.changefeedprocessor.services;

import com.microsoft.azure.documentdb.*;
import com.microsoft.azure.documentdb.changefeedprocessor.*;

public class DocumentServices {
    public DocumentServices(DocumentCollectionInfo collectionLocation) {
    }

    public DocumentServices createClient(String partitionId, String continuationToken) {
        return null;
    }

    public Object listPartitionRange() {
        return null;
    }

    public Object createDocumentChangeFeedQuery(String partitionId) {
        return null;
    }

}
