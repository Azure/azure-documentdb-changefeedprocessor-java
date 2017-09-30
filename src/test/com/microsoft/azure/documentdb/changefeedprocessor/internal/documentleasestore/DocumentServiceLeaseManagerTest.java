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
import org.junit.Test;
import static org.junit.Assert.*;

import java.io.Console;
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
    // TODO consider cleanup by deleting all leases and starting with a fresh lease store every time
    @Before
    public void init() throws DocumentClientException {
        
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
    }

    /**
     * Test of leaseStoreExists method, of class DocumentServiceLeaseManager.
     */
    @Test
    public void testLeaseStoreExists() throws Exception {
        System.out.println("leaseStoreExists");
        
        boolean result = instance.leaseStoreExists();
        assert(result);
    }

    /**
     * Test of createLeaseStoreIfNotExists method, of class DocumentServiceLeaseManager.
     * @throws DocumentClientException 
     */
    // TODO test creation of lease store if it doesn't already exist
    @Test
    public void testCreateLeaseStoreIfNotExists() throws DocumentClientException {
        System.out.println("createLeaseStoreIfNotExists");
        
        boolean result = instance.createLeaseStoreIfNotExists();
                
        // The lease store already exists, so this should be false
        assertFalse(result);
    }

    /**
     * Test of listLeases method, of class DocumentServiceLeaseManager.
     */
    // TODO test for exact number of leases once delete all is done
    @Test
    public void testListLeases() {
        System.out.println("listLeases");
        
        Iterable<DocumentServiceLease> result = instance.listLeases();  
        
        assert result.spliterator().getExactSizeIfKnown() != 0 : "no leases returned";
    }

    /**
     * Test of createLeaseIfNotExist method, of class DocumentServiceLeaseManager.
     */
    // TODO test returning true once delete is done
    @Test
    public void testCreateLeaseIfNotExist() throws Exception {
        System.out.println("createLeaseIfNotExist");
        
        String partitionId = "test";
        String continuationToken = "1234";
        boolean result = instance.createLeaseIfNotExist(partitionId, continuationToken);
        
        
        // A lease with this partitionId already exists so it should return false
        assertEquals(false, result);
    }

    /**
     * Test of getLease method, of class DocumentServiceLeaseManager.
     */
    @Test
    public void testGetLeaseReturnNull() throws Exception {
        System.out.println("getLease");
        
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
        System.out.println("getLease");
        
        // Expected results for a known lease with this partitionId
        String expPartitionId = "test";;
        
        DocumentServiceLease result = instance.getLease(expPartitionId);
        
        // Check that partitionId was set to ensure they were set properly
        assertEquals(expPartitionId, result.getPartitionId());
    }

    /**
     * Test of acquire method, of class DocumentServiceLeaseManager.
     */
    @Test
    public void testAcquire() throws Exception {
        System.out.println("acquire");
        
        // Expected result
        String expOwner = "testOwner";
        
        // Get a lease from the lease store and pass it to acquire 
        DocumentServiceLease result = instance.acquire(instance.getLease("test1"), expOwner);
        
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
        
        // Get a lease from the lease store where the owner is known to be "testOwner"
        DocumentServiceLease lease = instance.getLease("test1");
        long previousTimestamp = lease.getTs();
        
        DocumentServiceLease result = instance.renew(lease);
        
        // Check timestamp was updated and that the lease state is leased
        assert result.getTs() > previousTimestamp : "timestamp not updated";
        assertEquals(result.getState(), LeaseState.Leased);
    }

    /**
     * Test of release method, of class DocumentServiceLeaseManager.
     */
    @Test
    public void testRelease() throws Exception {
        System.out.println("release");
        
        // Get an owned lease from the lease store
        DocumentServiceLease lease = instance.getLease("test2");
        
        // When the lease is released, should return true
        boolean result = instance.release(lease);
        assertEquals(true, result);
        
        // Add owner back to reset for the next test
        instance.acquire(instance.getLease("test2"), "testOwner");
    }

    /**
     * Test of delete method, of class DocumentServiceLeaseManager.
     */
    @Test
    public void testDelete() throws Exception {
        System.out.println("delete");
        
        // Get a lease from the lease store and delete it
        instance.delete(instance.getLease("test3"));
        
        // This should return true if the lease was previously deleted
        // It will also ensure the lease is created to reset for the next time the test is run
        assert(instance.createLeaseIfNotExist("test3", "1234"));
    }

    /**
     * Test of deleteAll method, of class DocumentServiceLeaseManager.
     */
    @Test
    public void testDeleteAll() throws Exception {
        System.out.println("deleteAll");

        instance.deleteAll();
        
        // Listing all leases should return null
        long temp = instance.listLeases().spliterator().getExactSizeIfKnown();
        assert temp == 0 : "leases returned";
        
        // Create the necessary leases and set owners to reset other tests
        instance.createLeaseStoreIfNotExists();
        instance.createLeaseIfNotExist("test", "1234");
        instance.createLeaseIfNotExist("test1", "1234");
        instance.acquire(instance.getLease("test1"), "testOwner");
        instance.createLeaseIfNotExist("test2", "1234");
        instance.acquire(instance.getLease("test2"), "testOwner");
        instance.createLeaseIfNotExist("test3", "1234");
    }

    /**
     * Test of isExpired method, of class DocumentServiceLeaseManager.
     * @throws DocumentClientException 
     */
    @Test
    public void testIsExpired() throws DocumentClientException {
        System.out.println("isExpired");
        
        boolean result = instance.isExpired(instance.getLease("test"));
        assert(result);
    }

    /**
     * Test of checkpoint method, of class DocumentServiceLeaseManager.
     * @throws DocumentClientException 
     */
    @Test
    public void testCheckpoint() throws DocumentClientException {
        System.out.println("checkpoint");
        
        // Get an owned lease from the store
        DocumentServiceLease lease = instance.getLease("test1");
        
        String continuationToken = "12345";
        long sequenceNumber = 0L;
        Lease result = instance.checkpoint(lease, continuationToken, sequenceNumber);
        
        assertEquals(continuationToken, result.getContinuationToken());
        assertEquals(sequenceNumber, result.getSequenceNumber());
    }
    
}
