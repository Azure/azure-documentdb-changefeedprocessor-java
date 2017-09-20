package com.microsoft.azure.documentdb.changefeedprocessor;

import com.microsoft.azure.documentdb.ChangeFeedOptions;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.ConfigurationException;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.ConfigurationFile;
import org.junit.Test;

import java.net.URI;
import java.net.URISyntaxException;

public class ChangeFeedEventHostTest {

    @Test (expected = IllegalArgumentException.class)
    public void testInvalidConstructorHostname() throws IllegalArgumentException {
        ChangeFeedEventHost host = new ChangeFeedEventHost(
                null,
                new DocumentCollectionInfo(), new DocumentCollectionInfo(), new ChangeFeedOptions(), new ChangeFeedHostOptions());
    }
    @Test (expected = IllegalArgumentException.class)
    public void testInvalidConstructorDocumentCollectionInfo() throws IllegalArgumentException {
        ChangeFeedEventHost host = new ChangeFeedEventHost(
                "test",
                null, new DocumentCollectionInfo(), new ChangeFeedOptions(), new ChangeFeedHostOptions());
    }
    @Test (expected = IllegalArgumentException.class)
    public void testInvalidConstructorAuxCollectionInfo() throws IllegalArgumentException {
        ChangeFeedEventHost host = new ChangeFeedEventHost(
                "test",
                new DocumentCollectionInfo(), null, new ChangeFeedOptions(), new ChangeFeedHostOptions());
    }
    @Test (expected = IllegalArgumentException.class)
    public void testInvalidConstructorChangefeedOptions() throws IllegalArgumentException {
        ChangeFeedEventHost host = new ChangeFeedEventHost(
                "test",
                new DocumentCollectionInfo(), new DocumentCollectionInfo(), null, new ChangeFeedHostOptions());
    }
    @Test (expected = IllegalArgumentException.class)
    public void testInvalidConstructorChangefeedHostOptions() throws IllegalArgumentException {
        ChangeFeedEventHost host = new ChangeFeedEventHost(
                "test",
                new DocumentCollectionInfo(), new DocumentCollectionInfo(), new ChangeFeedOptions(), null);
    }


    @Test (expected = IllegalArgumentException.class)
    public void testCreatChangeFeedHostUsingSecrets() throws ConfigurationException, URISyntaxException {
        ConfigurationFile config = new ConfigurationFile("app.secrets");

        DocumentCollectionInfo docInfo = new DocumentCollectionInfo();
        docInfo.setUri(new URI(config.get("COSMOSDB_ENDPOINT")));
        docInfo.setMasterKey(config.get("COSMOSDB_SECRET"));
        docInfo.setDatabaseName(config.get("COSMOSDB_DATABASE"));
        docInfo.setCollectionName(config.get("COSMOSDB_COLLECTION"));

        DocumentCollectionInfo docAux = new DocumentCollectionInfo(docInfo);
        docAux.setCollectionName(config.get("COSMOSDB_AUX_COLLECTION"));

        ChangeFeedEventHost host = new ChangeFeedEventHost("hotsname", docInfo, docAux );
    }
}
