/*
 * The MIT License (MIT)
 * Copyright (c) 2018 Microsoft Corporation
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package com.microsoft.azure.documentdb.changefeedprocessor.internal.documentleasestore;

import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.changefeedprocessor.DocumentCollectionInfo;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.ConfigurationException;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.ConfigurationFile;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.Lease;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.LeaseLostException;
import com.microsoft.azure.documentdb.changefeedprocessor.services.DocumentServices;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.logging.Logger;

import static org.junit.Assert.*;

public class DocumentServiceLeaseTestManagerTest {
    private DocumentServiceLeaseManager instance = null;
    static final Duration DEFAULT_EXPIRATION_INTERVAL = Duration.ofSeconds(60);
    static final Duration DEFAULT_RENEW_INTERVAL = Duration.ofSeconds(17);
    private Logger logger = Logger.getLogger(DocumentServiceLeaseTestManagerTest.class.getName());

    public DocumentServiceLeaseTestManagerTest() {
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

        DocumentServices documentServices = new DocumentServices(docInfo);

        instance = new DocumentServiceLeaseManager(docInfo, "leases", DEFAULT_EXPIRATION_INTERVAL, DEFAULT_RENEW_INTERVAL,documentServices);
        instance.initialize(true);
        
        // Clean up lease store before each test
		instance.deleteAll();
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
        boolean result = instance.createLeaseIfNotExist(partitionId, continuationToken).call();
        
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
        instance.createLeaseIfNotExist(partitionId, continuationToken);  
        
        // Try to add the lease again, should return false since it already exists
        boolean result = instance.createLeaseIfNotExist(partitionId, continuationToken).call();
        
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
        instance.createLeaseIfNotExist(partitionId, continuationToken); 
        
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
        instance.createLeaseIfNotExist(partitionId, continuationToken);  

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
        instance.createLeaseIfNotExist(partitionId, continuationToken);  
        String expOwner = "testOwner";
        instance.acquire(instance.getLease(partitionId).call(), expOwner);
        
        // Get the lease back from the store and store the timestamp of it's last alteration time
        DocumentServiceLease lease = instance.getLease(partitionId).call();
        long previousTimestamp = lease.getTs();
        
        // Renew the lease for this owner
        DocumentServiceLease result = instance.renew(lease).call();
        
        // Check timestamp was updated and that the lease state is still leased
        assert result.getTs() > previousTimestamp : "timestamp not updated";
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
        instance.createLeaseIfNotExist(partitionId, continuationToken);  
        String expOwner = "testOwner";
        instance.acquire(instance.getLease(partitionId).call(), expOwner);
        
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
        instance.createLeaseIfNotExist(partitionId, continuationToken); 
        
        // Get the lease back from the lease store and delete it
        instance.delete(instance.getLease(partitionId).call());
        
        // This should return true if the lease was previously deleted
        assert(instance.createLeaseIfNotExist(partitionId, continuationToken).call());
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
        instance.createLeaseIfNotExist(partitionId, continuationToken);
        instance.createLeaseIfNotExist(partitionId2, continuationToken);
        
        // Delete all leases in the store
        instance.deleteAll();
        
        // Listing all leases should result in an empty list
        assert instance.listLeases().call().spliterator().getExactSizeIfKnown() == 0 : "leases returned";
    }

    /**
     * Test of isExpired method, of class DocumentServiceLeaseManager.
     * @throws DocumentClientException 
     * @throws InterruptedException 
     */
    @Test
    public void testIsExpiredTrue() throws DocumentClientException, InterruptedException {
        logger.info("isExpired");
        
        // Add a lease to the store
        String partitionId = "test";
        String continuationToken = "1234";
        instance.createLeaseIfNotExist(partitionId, continuationToken); 
        
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
     * @throws DocumentClientException 
     * @throws LeaseLostException 
     */
    @Test
    public void testIsExpiredFalse() throws DocumentClientException, LeaseLostException {
        logger.info("isExpired");
        
        // Add a lease to the store
        String partitionId = "test";
        String continuationToken = "1234";
        instance.createLeaseIfNotExist(partitionId, continuationToken); 
        
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
     * @throws DocumentClientException 
     */
    @Test
    public void testCheckpoint() throws DocumentClientException {
        logger.info("checkpoint");
        
        // Add a lease to the store and add an owner
        String partitionId = "test";
        String continuationToken = "1234";
        instance.createLeaseIfNotExist(partitionId, continuationToken);
        String expOwner = "testOwner";

        try {
            instance.acquire(instance.getLease(partitionId).call(), expOwner);
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
        Lease result = instance.checkpoint(lease, newContinuationToken, sequenceNumber);
        
        assertEquals(newContinuationToken, result.getContinuationToken());
        assertEquals(sequenceNumber, result.getSequenceNumber());
    }
}
