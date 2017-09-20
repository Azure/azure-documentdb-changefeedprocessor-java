package com.microsoft.azure.documentdb.changefeedprocessor;

import com.microsoft.azure.documentdb.ChangeFeedOptions;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.ConfigurationException;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.ConfigurationFile;
import org.junit.Assert;
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


    @Test
    public void testCreatChangeFeedHostUsingSecrets()  {
        ConfigurationFile config = null;

        try {
            config = new ConfigurationFile("app.secrets");
        } catch (ConfigurationException e) {
            Assert.fail(e.getMessage());
        }

        DocumentCollectionInfo docInfo = new DocumentCollectionInfo();
        try {
            docInfo.setUri(new URI(config.get("COSMOSDB_ENDPOINT")));
            docInfo.setMasterKey(config.get("COSMOSDB_SECRET"));
            docInfo.setDatabaseName(config.get("COSMOSDB_DATABASE"));
            docInfo.setCollectionName(config.get("COSMOSDB_COLLECTION"));
        } catch (URISyntaxException e) {
            Assert.fail("COSMOSDB URI FAIL: " + e.getMessage());
        } catch (ConfigurationException e) {
            Assert.fail("Configuration Error " + e.getMessage());

        }

        DocumentCollectionInfo docAux = new DocumentCollectionInfo(docInfo);

        try {
            docAux.setCollectionName(config.get("COSMOSDB_AUX_COLLECTION"));
        } catch (ConfigurationException e) {
            Assert.fail("Configuration Error " + e.getMessage());
        }

        ChangeFeedEventHost host = new ChangeFeedEventHost("hotsname", docInfo, docAux );
        Assert.assertNotNull(host);

        host.registerObserver(TestChangeFeedObserver.class);
    }
}
