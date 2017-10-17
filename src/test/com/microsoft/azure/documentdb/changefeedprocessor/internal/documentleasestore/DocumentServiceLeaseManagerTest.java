/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.microsoft.azure.documentdb.changefeedprocessor.internal.documentleasestore;

import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.changefeedprocessor.DocumentCollectionInfo;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.ConfigurationException;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.ConfigurationFile;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.Lease;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.LeaseLostException;

import org.junit.Test;
import static org.junit.Assert.*;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;

import org.junit.Assert;
import org.junit.Before;

/**
 *
 * @author yoterada
 */
public class DocumentServiceLeaseManagerTest {
    DocumentServiceLeaseManager instance = null;
    static final Duration DEFAULT_EXPIRATION_INTERVAL = Duration.ofSeconds(60);
    static final Duration DEFAULT_RENEW_INTERVAL = Duration.ofSeconds(17);
    
    public DocumentServiceLeaseManagerTest() {
    }
    
    // Setup before each test
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
        
        instance = new DocumentServiceLeaseManager(docInfo, "leases", DEFAULT_EXPIRATION_INTERVAL, DEFAULT_RENEW_INTERVAL);
        instance.initialize();
        
        // Clean up lease store before each test
		instance.deleteAll();
    }

    /**
     * Test of leaseStoreExists method, of class DocumentServiceLeaseManager.
     */
    @Test
    public void testLeaseStoreExists() throws Exception {
        System.out.println("leaseStoreExists");
        
        // The lease store exists, should be true
        boolean result = instance.leaseStoreExists();
        assertFalse(result);
    }

    /**
     * Test of createLeaseStoreIfNotExists method, of class DocumentServiceLeaseManager.
     * @throws DocumentClientException 
     */
    @Test
    public void testCreateLeaseStoreIfNotExists() throws DocumentClientException {
        System.out.println("createLeaseStoreIfNotExists");
        
        boolean result = instance.createLeaseStoreIfNotExists();
                
        // The lease store already exists, so this should be false
        assert(result);
    }

    /**
     * Test of listLeases method, of class DocumentServiceLeaseManager.
     * @throws DocumentClientException 
     */
    @Test
    public void testListLeases() throws DocumentClientException {
        System.out.println("listLeases");
        
        // Add two leases to the store
        String partitionId = "test";
        String partitionId2 = "test2";
        String continuationToken = "1234";
        instance.createLeaseIfNotExist(partitionId, continuationToken);
        instance.createLeaseIfNotExist(partitionId2, continuationToken);
                
        Iterable<DocumentServiceLease> result = instance.listLeases();  
        
        assert result.spliterator().getExactSizeIfKnown() == 2 : "incorrect number of leases returned";
    }

    /**
     * Test of createLeaseIfNotExist method, of class DocumentServiceLeaseManager.
     */
    @Test
    public void testCreateLeaseIfNotExistTrue() throws Exception {
        System.out.println("createLeaseIfNotExistTrue");
        
        String partitionId = "test";
        String continuationToken = "1234";
        boolean result = instance.createLeaseIfNotExist(partitionId, continuationToken);     
        
        assert(result);
    }
    
    /**
     * Test of createLeaseIfNotExist method, of class DocumentServiceLeaseManager.
     */
    @Test
    public void testCreateLeaseIfNotExistFalse() throws Exception {
        System.out.println("createLeaseIfNotExistFalse");
        
        // Add a lease to the store      
        String partitionId = "test";
        String continuationToken = "1234";
        instance.createLeaseIfNotExist(partitionId, continuationToken);  
        
        // Try to add the lease again, should return false since it already exists
        boolean result = instance.createLeaseIfNotExist(partitionId, continuationToken);
        
        // A lease with this partitionId already exists so it should return false
        assertFalse(result);
    }

    /**
     * Test of getLease method, of class DocumentServiceLeaseManager.
     */
    @Test
    public void testGetLeaseReturnNull() throws Exception {
        System.out.println("getLeaseReturnNull");
        
        // This partitionId doesn't exist in our lease store
        String partitionId = "nonExistant";
        
        DocumentServiceLease result = instance.getLease(partitionId);
        
        // Validate getLease returned null
        assertNull(result);
    }
    
    /**
     * Test of getLease method, of class DocumentServiceLeaseManager.
     */
    @Test
    public void testGetLeaseReturnLease() throws Exception {
        System.out.println("getLeaseReturnLease");
        
        // Add a lease to the store      
        String partitionId = "test";
        String continuationToken = "1234";
        instance.createLeaseIfNotExist(partitionId, continuationToken); 
        
        DocumentServiceLease result = instance.getLease(partitionId);
        
        // Check that partitionId was set to ensure they were set properly
        assertEquals(partitionId, result.getPartitionId());
    }

    /**
     * Test of acquire method, of class DocumentServiceLeaseManager.
     */
    @Test
    public void testAcquire() throws Exception {
        System.out.println("acquire");
        
        // Add a lease to the store      
        String partitionId = "test";
        String continuationToken = "1234";
        instance.createLeaseIfNotExist(partitionId, continuationToken);  

        // Get a lease from the lease store and pass it to acquire with owner name expOwner
        String expOwner = "testOwner";
        DocumentServiceLease result = instance.acquire(instance.getLease(partitionId), expOwner);
        
        // Ensure the owner was set properly
        assertEquals(expOwner, result.getOwner());
        assertEquals(LeaseState.Leased, result.getState());
    }

    /**
     * Test of renew method, of class DocumentServiceLeaseManager.
     */
    @Test
    public void testRenew() throws Exception {
        System.out.println("renew");
        
        // Add a lease to the store and add an owner
        String partitionId = "test";
        String continuationToken = "1234";
        instance.createLeaseIfNotExist(partitionId, continuationToken);  
        String expOwner = "testOwner";
        instance.acquire(instance.getLease(partitionId), expOwner);
        
        // Get the lease back from the store and store the timestamp of it's last alteration time
        DocumentServiceLease lease = instance.getLease(partitionId);
        long previousTimestamp = lease.getTs();
        
        // Renew the lease for this owner
        DocumentServiceLease result = instance.renew(lease);
        
        // Check timestamp was updated and that the lease state is still leased
        assert result.getTs() > previousTimestamp : "timestamp not updated";
        assertEquals(LeaseState.Leased, result.getState());
        assertEquals(expOwner, result.getOwner());
    }

    /**
     * Test of release method, of class DocumentServiceLeaseManager.
     */
    @Test
    public void testRelease() throws Exception {
        System.out.println("release");
        
        // Get an owned lease from the lease store
        // Add a lease to the store and add an owner
        String partitionId = "test";
        String continuationToken = "1234";
        instance.createLeaseIfNotExist(partitionId, continuationToken);  
        String expOwner = "testOwner";
        instance.acquire(instance.getLease(partitionId), expOwner);
        
        // Get the lease back from the store
        DocumentServiceLease lease = instance.getLease(partitionId);
        
        // When the lease is released, should return true
        boolean result = instance.release(lease);
        
        // Get the lease one more time to ensure the state was updated
        DocumentServiceLease leaseAfterRelease = instance.getLease(partitionId);
        
        assert(result);
        assertEquals(LeaseState.Available, leaseAfterRelease.getState());
    }

    /**
     * Test of delete method, of class DocumentServiceLeaseManager.
     */
    @Test
    public void testDelete() throws Exception {
        System.out.println("delete");
        
        // Add a lease to the store
        String partitionId = "test";
        String continuationToken = "1234";
        instance.createLeaseIfNotExist(partitionId, continuationToken); 
        
        // Get the lease back from the lease store and delete it
        instance.delete(instance.getLease(partitionId));
        
        // This should return true if the lease was previously deleted
        assert(instance.createLeaseIfNotExist(partitionId, continuationToken));
    }

    /**
     * Test of deleteAll method, of class DocumentServiceLeaseManager.
     */
    @Test
    public void testDeleteAll() throws Exception {
        System.out.println("deleteAll");

        // Add a couple leases to the store
        String partitionId = "test";
        String partitionId2 = "test2";
        String continuationToken = "1234";
        instance.createLeaseIfNotExist(partitionId, continuationToken);
        instance.createLeaseIfNotExist(partitionId2, continuationToken);
        
        // Delete all leases in the store
        instance.deleteAll();
        
        // Listing all leases should result in an empty list
        assert instance.listLeases().spliterator().getExactSizeIfKnown() == 0 : "leases returned";
    }

    /**
     * Test of isExpired method, of class DocumentServiceLeaseManager.
     * @throws DocumentClientException 
     * @throws InterruptedException 
     */
    @Test
    public void testIsExpiredTrue() throws DocumentClientException, InterruptedException {
        System.out.println("isExpired");
        
        // Add a lease to the store
        String partitionId = "test";
        String continuationToken = "1234";
        instance.createLeaseIfNotExist(partitionId, continuationToken); 
        
        // Wait 1 min for the lease to expire
        Thread.sleep(60000);
          
        // This lease was previously added to the store and should be expired
        DocumentServiceLease temp = instance.getLease(partitionId);
        boolean result = instance.isExpired(temp);
        assert(result);
    }
    
    /**
     * Test of isExpired method, of class DocumentServiceLeaseManager.
     * @throws DocumentClientException 
     * @throws LeaseLostException 
     */
    @Test
    public void testIsExpiredFalse() throws DocumentClientException, LeaseLostException {
        System.out.println("isExpired");
        
        // Add a lease to the store
        String partitionId = "test";
        String continuationToken = "1234";
        instance.createLeaseIfNotExist(partitionId, continuationToken); 
        
        // The new lease shouldn't be expired yet
        boolean result = instance.isExpired(instance.getLease(partitionId));
        assertFalse(result);
    }

    /**
     * Test of checkpoint method, of class DocumentServiceLeaseManager.
     * @throws DocumentClientException 
     */
    @Test
    public void testCheckpoint() throws DocumentClientException {
        System.out.println("checkpoint");
        
        // Add a lease to the store and add an owner
        String partitionId = "test";
        String continuationToken = "1234";
        instance.createLeaseIfNotExist(partitionId, continuationToken);
        String expOwner = "testOwner";
        instance.acquire(instance.getLease(partitionId), expOwner);
        
        // Get an owned lease from the store
        DocumentServiceLease lease = instance.getLease(partitionId);
        
        String newContinuationToken = "12345";
        long sequenceNumber = 0L;
        Lease result = instance.checkpoint(lease, newContinuationToken, sequenceNumber);
        
        assertEquals(newContinuationToken, result.getContinuationToken());
        assertEquals(sequenceNumber, result.getSequenceNumber());
    }
}
