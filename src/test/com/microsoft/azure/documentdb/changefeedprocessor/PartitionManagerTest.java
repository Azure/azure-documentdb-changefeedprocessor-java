package com.microsoft.azure.documentdb.changefeedprocessor;

import java.net.URI;
import java.net.URISyntaxException;

import org.junit.Assert;
import org.junit.Before;

import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.ConfigurationException;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.ConfigurationFile;
import com.microsoft.azure.documentdb.changefeedprocessor.services.DocumentCollectionInfo;
import com.microsoft.azure.documentdb.changefeedprocessor.DocumentServiceLeaseManager;
import com.microsoft.azure.documentdb.changefeedprocessor.LeaseLostException;
import com.microsoft.azure.documentdb.changefeedprocessor.services.DocumentServices;

public class PartitionManagerTest {
    private PartitionManager instance = null;
    private DocumentServiceLeaseManager leaseManager = null;

    public PartitionManagerTest() {
    }
    
    // TODO: Fix this!
    @Before
    public void init() throws DocumentClientException, LeaseLostException {
        
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
            docInfo.setCollectionName(config.get("COSMOSDB_LEASE_COLLECTION"));
        } catch (URISyntaxException e) {
            Assert.fail("COSMOSDB URI FAIL: " + e.getMessage());
        } catch (ConfigurationException e) {
            Assert.fail("Configuration Error " + e.getMessage());

        }

        DocumentServices documentServices = new DocumentServices(docInfo);

        //leaseManager = new DocumentServiceLeaseManager(docInfo, "leases", DEFAULT_EXPIRATION_INTERVAL, DEFAULT_RENEW_INTERVAL,documentServices);
        //leaseManager.initialize();
    }
}
