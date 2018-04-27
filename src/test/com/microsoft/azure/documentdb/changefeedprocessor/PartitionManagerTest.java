package com.microsoft.azure.documentdb.changefeedprocessor;

import static org.junit.Assert.assertEquals;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.time.Instant;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.ConfigurationException;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.ConfigurationFile;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.LeaseState;
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
    public void init() throws Exception {
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
        
        // Delete all previous leases from the store to prep for the next test
        leaseManager.deleteAll().call();
    	
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
    
    // TODO: This test works when stepping through with the debugger but not when letting it run naturally
    // Some call in this chain must not be blocking properly
    @Test
    public void testIntializeRenewLease() throws Exception {
    	// Instantiate a Partition Manager
        instance = new PartitionManager<DocumentServiceLease>(OWNER_NAME, leaseManager, options);
    	
    	// Add a lease to the store
    	String p1 = "p1";
    	String contToken = "1234";
    	leaseManager.createLeaseIfNotExists(p1, contToken).call();
    	
    	// Assign the lease to this partition manager
    	DocumentServiceLease lease = leaseManager.getLease(p1).call();
    	Instant previousTimestamp = lease.getTimestamp();
    	leaseManager.acquire(lease, OWNER_NAME).call();
    	
    	// Initialize the partition manager
    	instance.initialize();
    	
    	// Get the lease back from the store
    	DocumentServiceLease result = leaseManager.getLease(p1).call();
    	
    	// Verify the lease still belongs to this partition manager and the timestamp was updated
    	assert(instance.currentlyOwnedPartitions.size() == 1);
        assert result.getTimestamp().isAfter(previousTimestamp) : "timestamp not updated";
        assertEquals(LeaseState.LEASED, result.getState());
        assertEquals(OWNER_NAME, result.getOwner());
    }
    
    @Test
    public void testIntializeNotRenewLease() throws Exception {
    	// Instantiate a Partition Manager
        instance = new PartitionManager<DocumentServiceLease>(OWNER_NAME, leaseManager, options);
    	
    	// Add a lease to the store
    	String p1 = "p1";
    	String contToken = "1234";
    	leaseManager.createLeaseIfNotExists(p1, contToken).call();
    	
    	// Initialize the partition manager
    	instance.initialize();
    	
    	// Get the lease back from the store
    	DocumentServiceLease result = leaseManager.getLease(p1).call();
    	
    	// Verify this partition manager still doesn't own any leases
    	assert(instance.currentlyOwnedPartitions.size() == 0);
        assertEquals(null, result.getState());
        assertEquals("", result.getOwner());
    }
    
    @Test
    public void testStartTakerTask() throws Exception {
    	// Instantiate a Partition Manager
        instance = new PartitionManager<DocumentServiceLease>(OWNER_NAME, leaseManager, options);
    	
    	// Add a lease to the store
    	String p1 = "p1";
    	String contToken = "1234";
    	leaseManager.createLeaseIfNotExists(p1, contToken).call();
    	
    	// Initialize the partition manager
    	instance.initialize();
    	
    	// Start the partition manager, triggering the lease taker task
    	instance.start();
    	
    	// Get the lease back from the store
    	DocumentServiceLease result = leaseManager.getLease(p1).call();
    	
    	// Verify this partition manager still doesn't own any leases
    	assert(instance.currentlyOwnedPartitions.size() == 1);
        assertEquals(LeaseState.LEASED, result.getState());
        assertEquals(OWNER_NAME, result.getOwner());
    }
}
