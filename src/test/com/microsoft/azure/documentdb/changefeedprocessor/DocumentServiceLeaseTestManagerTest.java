/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.microsoft.azure.documentdb.changefeedprocessor;

import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.changefeedprocessor.DocumentServiceLeaseManager;
import com.microsoft.azure.documentdb.changefeedprocessor.services.DocumentCollectionInfo;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.ConfigurationException;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.ConfigurationFile;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.LeaseState;
import com.microsoft.azure.documentdb.changefeedprocessor.Lease;
import com.microsoft.azure.documentdb.changefeedprocessor.LeaseLostException;
import com.microsoft.azure.documentdb.changefeedprocessor.services.DocumentServices;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.time.Instant;
import java.util.logging.Logger;

import static org.junit.Assert.*;

/**
 *
 * @author yoterada
 */
public class DocumentServiceLeaseTestManagerTest {
    private DocumentServiceLeaseManager instance = null;
    static final Duration DEFAULT_EXPIRATION_INTERVAL = Duration.ofSeconds(60);
    static final Duration DEFAULT_RENEW_INTERVAL = Duration.ofSeconds(17);
    private Logger logger = Logger.getLogger(DocumentServiceLeaseTestManagerTest.class.getName());

    public DocumentServiceLeaseTestManagerTest() {
    }
    
    // Setup before each test
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

        instance = new DocumentServiceLeaseManager(docInfo, "leases", DEFAULT_EXPIRATION_INTERVAL, DEFAULT_RENEW_INTERVAL,documentServices);
        instance.initialize();
        
        // Clean up lease store before each test
		instance.deleteAll().call();
    }

    /**
     * Test of leaseStoreExists method, of class DocumentServiceLeaseManager.
     */
    @Test
    public void testLeaseStoreExists() throws Exception {

        logger.info("leaseStoreExists");
        
        // The lease store exists, should be true
        boolean result = instance.leaseStoreExists().call();

        assertFalse(result);
    }

    /**
     * Test of createLeaseStoreIfNotExists method, of class DocumentServiceLeaseManager.
     * @throws DocumentClientException 
     */
    @Test
    public void testCreateLeaseStoreIfNotExists() throws DocumentClientException {
        logger.info("createLeaseStoreIfNotExists");

        boolean result = false;
        try {
            result = instance.createLeaseStoreIfNotExists().call();
        } catch (Exception e) {
            e.printStackTrace();
            logger.warning(e.getMessage());
        }

        // The lease store already exists, so this should be false
        assert(result);
    }

    /**
     * Test of listLeases method, of class DocumentServiceLeaseManager.
     * @throws Exception 
     */
    @Test
    public void testListLeases() throws Exception {
        System.out.println("listLeases");
        
        // Add two leases to the store
        String partitionId = "test";
        String partitionId2 = "test2";
        String continuationToken = "1234";
        instance.createLeaseIfNotExists(partitionId, continuationToken).call();
        instance.createLeaseIfNotExists(partitionId2, continuationToken).call();

        Iterable<DocumentServiceLease> result = null;
        try {
            result = instance.listLeases().call();
        } catch (Exception e) {
            e.printStackTrace();
            logger.warning(e.getMessage());
        }

        assert result.spliterator().getExactSizeIfKnown() == 2 : "incorrect number of leases returned";
    }

    /**
     * Test of createLeaseIfNotExist method, of class DocumentServiceLeaseManager.
     */
    @Test
    public void testCreateLeaseIfNotExistTrue() throws Exception {
        logger.info("createLeaseIfNotExistTrue");
        
        String partitionId = "test";
        String continuationToken = "1234";
        boolean result = instance.createLeaseIfNotExists(partitionId, continuationToken).call();
        
        assert(result);
    }
    
    /**
     * Test of createLeaseIfNotExist method, of class DocumentServiceLeaseManager.
     */
    @Test
    public void testCreateLeaseIfNotExistFalse() throws Exception {
        logger.info("createLeaseIfNotExistFalse");
        
        // Add a lease to the store      
        String partitionId = "test";
        String continuationToken = "1234";
        instance.createLeaseIfNotExists(partitionId, continuationToken).call();  
        
        // Try to add the lease again, should return false since it already exists
        boolean result = instance.createLeaseIfNotExists(partitionId, continuationToken).call();
        
        // A lease with this partitionId already exists so it should return false
        assertFalse(result);
    }

    /**
     * Test of getLease method, of class DocumentServiceLeaseManager.
     */
    @Test
    public void testGetLeaseReturnNull() throws Exception {
       logger.info("getLeaseReturnNull");
        
        // This partitionId doesn't exist in our lease store
        String partitionId = "nonExistant";
        
        DocumentServiceLease result = instance.getLease(partitionId).call();
        
        // Validate getLease returned null
        assertNull(result);
    }
    
    /**
     * Test of getLease method, of class DocumentServiceLeaseManager.
     */
    @Test
    public void testGetLeaseReturnLease() throws Exception {
        logger.info("getLeaseReturnLease");
        
        // Add a lease to the store      
        String partitionId = "test";
        String continuationToken = "1234";
        instance.createLeaseIfNotExists(partitionId, continuationToken).call(); 
        
        DocumentServiceLease result = instance.getLease(partitionId).call();
        
        // Check that partitionId was set to ensure they were set properly
        assertEquals(partitionId, result.getPartitionId());
    }

    /**
     * Test of acquire method, of class DocumentServiceLeaseManager.
     */
    @Test
    public void testAcquire() throws Exception {
        logger.info("acquire");
        
        // Add a lease to the store      
        String partitionId = "test";
        String continuationToken = "1234";
        instance.createLeaseIfNotExists(partitionId, continuationToken).call();  

        // Get a lease from the lease store and pass it to acquire with owner name expOwner
        String expOwner = "testOwner";
        DocumentServiceLease result = instance.acquire(instance.getLease(partitionId).call(), expOwner).call();
        
        // Ensure the owner was set properly
        assertEquals(expOwner, result.getOwner());
        assertEquals(LeaseState.LEASED, result.getState());
    }

    /**
     * Test of renew method, of class DocumentServiceLeaseManager.
     */
    @Test
    public void testRenew() throws Exception {
        logger.info("renew");
        
        // Add a lease to the store and add an owner
        String partitionId = "test";
        String continuationToken = "1234";
        instance.createLeaseIfNotExists(partitionId, continuationToken).call();  
        String expOwner = "testOwner";
        instance.acquire(instance.getLease(partitionId).call(), expOwner).call();
        
        // Get the lease back from the store and store the timestamp of it's last alteration time
        DocumentServiceLease lease = instance.getLease(partitionId).call();
        Instant previousTimestamp = lease.getTimestamp();
        
        // Renew the lease for this owner
        DocumentServiceLease result = instance.renew(lease).call();
        
        // Check timestamp was updated and that the lease state is still leased
        assert result.getTimestamp().isAfter(previousTimestamp) : "timestamp not updated";
        assertEquals(LeaseState.LEASED, result.getState());
        assertEquals(expOwner, result.getOwner());
    }

    /**
     * Test of release method, of class DocumentServiceLeaseManager.
     */
    @Test
    public void testRelease() throws Exception {
        logger.info("release");
        
        // Get an owned lease from the lease store
        // Add a lease to the store and add an owner
        String partitionId = "test";
        String continuationToken = "1234";
        instance.createLeaseIfNotExists(partitionId, continuationToken).call();  
        String expOwner = "testOwner";
        instance.acquire(instance.getLease(partitionId).call(), expOwner).call();
        
        // Get the lease back from the store
        DocumentServiceLease lease = instance.getLease(partitionId).call();
        
        // When the lease is released, should return true
        boolean result = instance.release(lease).call();
        
        // Get the lease one more time to ensure the state was updated
        DocumentServiceLease leaseAfterRelease = instance.getLease(partitionId).call();
        
        assert(result);
        assertEquals(LeaseState.AVAILABLE, leaseAfterRelease.getState());
    }

    /**
     * Test of delete method, of class DocumentServiceLeaseManager.
     */
    @Test
    public void testDelete() throws Exception {
        logger.info("delete");
        
        // Add a lease to the store
        String partitionId = "test";
        String continuationToken = "1234";
        instance.createLeaseIfNotExists(partitionId, continuationToken).call(); 
        
        // Get the lease back from the lease store and delete it
        instance.delete(instance.getLease(partitionId).call()).call();
        
        // This should return true if the lease was previously deleted
        assert(instance.createLeaseIfNotExists(partitionId, continuationToken).call());
    }

    /**
     * Test of deleteAll method, of class DocumentServiceLeaseManager.
     */
    @Test
    public void testDeleteAll() throws Exception {
        logger.info("deleteAll");

        // Add a couple leases to the store
        String partitionId = "test";
        String partitionId2 = "test2";
        String continuationToken = "1234";
        instance.createLeaseIfNotExists(partitionId, continuationToken).call();
        instance.createLeaseIfNotExists(partitionId2, continuationToken).call();
        
        // Delete all leases in the store
        instance.deleteAll().call();
        
        // Listing all leases should result in an empty list
        assert instance.listLeases().call().spliterator().getExactSizeIfKnown() == 0 : "leases returned";
    }

    /**
     * Test of isExpired method, of class DocumentServiceLeaseManager.
     * @throws Exception 
     */
    @Test
    public void testIsExpiredTrue() throws Exception {
        logger.info("isExpired");
        
        // Add a lease to the store
        String partitionId = "test";
        String continuationToken = "1234";
        instance.createLeaseIfNotExists(partitionId, continuationToken).call(); 
        
        // Wait 1 min for the lease to expire
        Thread.sleep(60000);
          
        // This lease was previously added to the store and should be expired
        DocumentServiceLease temp = null;
        boolean result = false;
        try {
            temp = instance.getLease(partitionId).call();
            result = instance.isExpired(temp).call();
        } catch (Exception e) {
            e.printStackTrace();
        }

        assert(result);
    }
    
    /**
     * Test of isExpired method, of class DocumentServiceLeaseManager.
     * @throws Exception 
     */
    @Test
    public void testIsExpiredFalse() throws Exception {
        logger.info("isExpired");
        
        // Add a lease to the store
        String partitionId = "test";
        String continuationToken = "1234";
        instance.createLeaseIfNotExists(partitionId, continuationToken).call(); 
        
        // The new lease shouldn't be expired yet
        boolean result = false;
        try {
            result = instance.isExpired(instance.getLease(partitionId).call()).call();
        } catch (Exception e) {
            e.printStackTrace();
        }
        assertFalse(result);
    }

    /**
     * Test of checkpoint method, of class DocumentServiceLeaseManager.
     * @throws Exception 
     */
    @Test
    public void testCheckpoint() throws Exception {
        logger.info("checkpoint");
        
        // Add a lease to the store and add an owner
        String partitionId = "test";
        String continuationToken = "1234";
        instance.createLeaseIfNotExists(partitionId, continuationToken).call();
        String expOwner = "testOwner";

        try {
            instance.acquire(instance.getLease(partitionId).call(), expOwner).call();
        } catch (Exception e) {
            e.printStackTrace();
        }

        // Get an owned lease from the store
        DocumentServiceLease lease = null;
        try {
            lease = instance.getLease(partitionId).call();
        } catch (Exception e) {
            e.printStackTrace();
        }

        String newContinuationToken = "12345";
        long sequenceNumber = 0L;
        Lease result = instance.checkpoint(lease, newContinuationToken, sequenceNumber).call();
        
        assertEquals(newContinuationToken, result.getContinuationToken());
        assertEquals(sequenceNumber, result.getSequenceNumber());
    }
}
