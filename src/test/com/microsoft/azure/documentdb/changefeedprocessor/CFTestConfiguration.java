package com.microsoft.azure.documentdb.changefeedprocessor;

import com.microsoft.azure.documentdb.changefeedprocessor.internal.ConfigurationException;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.ConfigurationFile;
import com.microsoft.azure.documentdb.changefeedprocessor.services.DocumentServices;
import org.junit.Assert;

import java.net.URI;
import java.net.URISyntaxException;

public class CFTestConfiguration {

    private static DocumentCollectionInfo defaultDocumentCollectionInfo;

    public static DocumentCollectionInfo getDefaultDocumentCollectionInfo() {
        if(defaultDocumentCollectionInfo == null) {
            defaultDocumentCollectionInfo = getDocumentCollectionInfo();
        }

        return defaultDocumentCollectionInfo;
    }

    private static DocumentCollectionInfo getDocumentCollectionInfo() {

        ConfigurationFile config = null;
        DocumentServices client = null;

        DocumentCollectionInfo docInfo = new DocumentCollectionInfo();
        try {
            config = new ConfigurationFile("app.secrets");

            docInfo.setUri(new URI(config.get("COSMOSDB_ENDPOINT")));
            docInfo.setMasterKey(config.get("COSMOSDB_SECRET"));
            docInfo.setDatabaseName(config.get("COSMOSDB_DATABASE"));
            docInfo.setCollectionName(config.get("COSMOSDB_COLLECTION"));
        } catch (URISyntaxException | ConfigurationException e) {
            docInfo = null;
            e.printStackTrace();
        }

        return docInfo;
    }
}
