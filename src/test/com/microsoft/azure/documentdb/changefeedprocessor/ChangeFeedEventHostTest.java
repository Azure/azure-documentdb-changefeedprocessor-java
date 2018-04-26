package com.microsoft.azure.documentdb.changefeedprocessor;

import com.microsoft.azure.documentdb.ChangeFeedOptions;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.ConfigurationException;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.ConfigurationFile;
import com.microsoft.azure.documentdb.changefeedprocessor.services.DocumentCollectionInfo;

import org.junit.Assert;
import org.junit.Test;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class ChangeFeedEventHostTest {

    Logger logger = Logger.getLogger(ChangeFeedEventHostTest.class.getName());

    @Test (expected = IllegalArgumentException.class)
    public void testInvalidConstructorHostname() throws IllegalArgumentException {
        @SuppressWarnings("unused")
		ChangeFeedEventHost host = new ChangeFeedEventHost(
                null,
                new DocumentCollectionInfo(), new DocumentCollectionInfo(), new ChangeFeedOptions(), new ChangeFeedHostOptions());
    }
    @Test (expected = IllegalArgumentException.class)
    public void testInvalidConstructorDocumentCollectionInfo() throws IllegalArgumentException {
        @SuppressWarnings("unused")
		ChangeFeedEventHost host = new ChangeFeedEventHost(
                "test",
                null, new DocumentCollectionInfo(), new ChangeFeedOptions(), new ChangeFeedHostOptions());
    }
    @Test (expected = IllegalArgumentException.class)
    public void testInvalidConstructorAuxCollectionInfo() throws IllegalArgumentException {
        @SuppressWarnings("unused")
		ChangeFeedEventHost host = new ChangeFeedEventHost(
                "test",
                new DocumentCollectionInfo(), null, new ChangeFeedOptions(), new ChangeFeedHostOptions());
    }
    @Test (expected = IllegalArgumentException.class)
    public void testInvalidConstructorChangefeedOptions() throws IllegalArgumentException {
        @SuppressWarnings("unused")
		ChangeFeedEventHost host = new ChangeFeedEventHost(
                "test",
                new DocumentCollectionInfo(), new DocumentCollectionInfo(), null, new ChangeFeedHostOptions());
    }
    @Test (expected = IllegalArgumentException.class)
    public void testInvalidConstructorChangefeedHostOptions() throws IllegalArgumentException {
        @SuppressWarnings("unused")
		ChangeFeedEventHost host = new ChangeFeedEventHost(
                "test",
                new DocumentCollectionInfo(), new DocumentCollectionInfo(), new ChangeFeedOptions(), null);
    }


    @Test
    public void createChangeFeedHostUsingSecrets()  {
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

        ChangeFeedOptions options = new ChangeFeedOptions();
        options.setPageSize(100);

        ChangeFeedHostOptions hostOptions = new ChangeFeedHostOptions();
        hostOptions.setDiscardExistingLeases(true);

        ChangeFeedEventHost host = new ChangeFeedEventHost("hostname", docInfo, docAux, options, hostOptions );
        Assert.assertNotNull(host);

        try {
            host.registerObserver(MockChangeFeedObserver.class);

            // TODO: This will run indefinitely, fix when shutdown is implemented
            while(!host.getExecutorService().isTerminated() &&
                    !host.getExecutorService().isShutdown()){
                logger.info("Host Service is Running");
                host.getExecutorService().awaitTermination(1, TimeUnit.MINUTES);
            }
        }
        catch(Exception e) {
            Assert.fail("registerObserver exception " + e.getMessage());
        }
    }
}
