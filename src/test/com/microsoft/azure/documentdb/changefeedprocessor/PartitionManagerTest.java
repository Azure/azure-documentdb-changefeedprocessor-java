package com.microsoft.azure.documentdb.changefeedprocessor;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.ConfigurationException;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.ConfigurationFile;
import com.microsoft.azure.documentdb.changefeedprocessor.services.DocumentCollectionInfo;
import com.microsoft.azure.documentdb.changefeedprocessor.DocumentServiceLeaseManager;
import com.microsoft.azure.documentdb.changefeedprocessor.LeaseLostException;
import com.microsoft.azure.documentdb.changefeedprocessor.services.DocumentServices;

public class PartitionManagerTest {

    private PartitionManager<DocumentServiceLease> instance = null;
    private ChangeFeedHostOptions options = null;
    private LeaseManagerInterface<DocumentServiceLease> leaseManager = null;
    static final Duration DEFAULT_EXPIRATION_INTERVAL = Duration.ofSeconds(60);
    static final Duration DEFAULT_RENEW_INTERVAL = Duration.ofSeconds(17);
    static final String OWNER_NAME = "worker";
    
    public PartitionManagerTest() {
    }
    
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

        leaseManager = new DocumentServiceLeaseManager(docInfo, "leases", DEFAULT_EXPIRATION_INTERVAL, DEFAULT_RENEW_INTERVAL, documentServices);
        leaseManager.initialize();
    	
    	options = new ChangeFeedHostOptions();
    }
    
    @Test (expected = IllegalArgumentException.class)
    public void testOwnerNameNull() {
    	instance = new PartitionManager<DocumentServiceLease>(null, leaseManager, options);
    }
    
    @Test (expected = IllegalArgumentException.class)
    public void testLeaseManagerNull() {
    	instance = new PartitionManager<DocumentServiceLease>(OWNER_NAME, null, options);
    }
    
    @Test (expected = IllegalArgumentException.class)
    public void testOptionsNull() {
    	instance = new PartitionManager<DocumentServiceLease>(OWNER_NAME, leaseManager, null);
    }
    
    @Test
    public void testIntializeRenewLease() throws Exception {
    	// Instantiate a Partition Manager
        instance = new PartitionManager<DocumentServiceLease>(OWNER_NAME, leaseManager, options);
    	
    	// Add two leases to the store
    	String p1 = "p1";
    	String p2 = "p2";
    	String contToken = "1234";
    	leaseManager.createLeaseIfNotExists(p1, contToken).call();
    	leaseManager.createLeaseIfNotExists(p2, contToken).call();
    	
    	// Assign one lease to this partition manager
    	leaseManager.acquire(leaseManager.getLease(p1).call(), OWNER_NAME).call();
    	
    	// Initialize the partition manager
    	instance.initialize().call();
    	
    	// Verify both leases now belong to this partition manager
    	assert(instance.currentlyOwnedPartitions.size() == 2);
    }
}
