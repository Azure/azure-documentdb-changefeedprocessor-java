package com.microsoft.azure.documentdb.changefeedprocessor.services;

import com.microsoft.azure.documentdb.*;
import com.microsoft.azure.documentdb.changefeedprocessor.*;

public class DocumentServices {

    private final String _url;
    private final String _database;
    private final String _collection;
    private final String _masterKey;

    public DocumentServices(DocumentCollectionInfo collectionLocation) {
        this._url = collectionLocation.getUri().toString();
        this._database = collectionLocation.getDatabaseName();
        this._collection = collectionLocation.getCollectionName();
        this._masterKey = collectionLocation.getMasterKey();
    }

    public DocumentServicesClient createClient() {
        return new DocumentServicesClient(_url, _database, _collection, _masterKey);
    }

    public DocumentServices createClient(String partitionId, String continuationToken) {
        return null;
    }
}
