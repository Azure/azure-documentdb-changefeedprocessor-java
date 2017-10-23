package com.microsoft.azure.documentdb.changefeedprocessor;

import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.ConfigurationException;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.ConfigurationFile;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.documentleasestore.DocumentServiceLeaseManager;
import org.junit.Assert;
import org.junit.Test;

import java.net.URI;
import java.net.URISyntaxException;

public class LeaseTest {

    @Test
    public void LeaseInitialization() {
        //fail("Not yet implemented");

        ConfigurationFile config = null;

        try {
            config = new ConfigurationFile("app.secrets");
        } catch (ConfigurationException e) {
            Assert.fail(e.getMessage());
        }

        DocumentCollectionInfo docAux = new DocumentCollectionInfo();
        try {
            docAux.setUri(new URI(config.get("COSMOSDB_ENDPOINT")));
            docAux.setMasterKey(config.get("COSMOSDB_SECRET"));
            docAux.setDatabaseName(config.get("COSMOSDB_DATABASE"));
            docAux.setCollectionName(config.get("COSMOSDB_LEASE_COLLECTION"));
        } catch (URISyntaxException e) {
            Assert.fail("COSMOSDB URI FAIL: " + e.getMessage());
        } catch (ConfigurationException e) {
            Assert.fail("Configuration Error " + e.getMessage());
        }

        ChangeFeedHostOptions options = new ChangeFeedHostOptions();

        DocumentServiceLeaseManager leaseManager = new DocumentServiceLeaseManager(
                docAux,
                "lease",
                options.getLeaseExpirationInterval(),
                options.getLeaseRenewInterval());

        try {
            leaseManager.initialize(true);
        } catch (DocumentClientException e) {
            e.printStackTrace();

            Assert.fail(e.getMessage());
        }

    }






}
