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
        
        instance = new DocumentServiceLeaseManager(docInfo, "", DEFAULT_EXPIRATION_INTERVAL, DEFAULT_RENEW_INTERVAL);
        instance.initialize();
    }

    /**
     * Test of leaseStoreExists method, of class DocumentServiceLeaseManager.
     */
    @Test
    public void testLeaseStoreExists() throws Exception {
        System.out.println("leaseStoreExists");
        
        boolean expResult = true;
        boolean result = instance.leaseStoreExists();
        assertEquals(expResult, result);
    }

    /**
     * Test of createLeaseStoreIfNotExists method, of class DocumentServiceLeaseManager.
     * @throws DocumentClientException 
     */
    @Test
    public void testCreateLeaseStoreIfNotExists() throws DocumentClientException {
        System.out.println("createLeaseStoreIfNotExists");
        
        // The lease store already exists, so this should be false
        boolean expResult = false;
        boolean result = instance.createLeaseStoreIfNotExists();
        assertEquals(expResult, result);
    }

    /**
     * Test of listLeases method, of class DocumentServiceLeaseManager.
     */
    @Test
    public void testListLeases() {
        System.out.println("listLeases");
        
        Iterable<DocumentServiceLease> result = instance.listLeases();  
        
        assert result.spliterator().getExactSizeIfKnown() != 0 : "no leases returned";
    }

    /**
     * Test of createLeaseIfNotExist method, of class DocumentServiceLeaseManager.
     */
    @Test
    public void testCreateLeaseIfNotExist() throws Exception {
        System.out.println("createLeaseIfNotExist");
        
        // This lease already exists so it should be false
        String partitionId = "test";
        String continuationToken = "1234";
        boolean expResult = false;
        boolean result = instance.createLeaseIfNotExist(partitionId, continuationToken);
        assertEquals(expResult, result);
    }

    /**
     * Test of getLease method, of class DocumentServiceLeaseManager.
     */
    @Test
    public void testGetLeaseReturnNull() throws Exception {
        System.out.println("getLease");
        
        String partitionId = "nonExistant";
        DocumentServiceLease expResult = null;
        DocumentServiceLease result = instance.getLease(partitionId);
        assertEquals(expResult, result);
    }
    
    /**
     * Test of getLease method, of class DocumentServiceLeaseManager.
     */
    @Test
    public void testGetLeaseReturnLease() throws Exception {
        System.out.println("getLease");
        
        String partitionId = "test1";
        DocumentServiceLease expResult = new DocumentServiceLease();
        expResult.setPartitionId("test1");
        expResult.setTs(1506106003);;
        DocumentServiceLease result = instance.getLease(partitionId);
        assertEquals(expResult.getTs(), result.getTs());
    }

//    /**
//     * Test of acquire method, of class DocumentServiceLeaseManager.
//     */
//    @Test
//    public void testAcquire() throws Exception {
//        System.out.println("acquire");
//        DocumentServiceLease lease = null;
//        String owner = "";
//        DocumentServiceLeaseManager instance = null;
//        DocumentServiceLease expResult = null;
//        DocumentServiceLease result = instance.acquire(lease, owner);
//        assertEquals(expResult, result);
//        // TODO review the generated test code and remove the default call to fail.
//        fail("The test case is a prototype.");
//    }
//
//    /**
//     * Test of renew method, of class DocumentServiceLeaseManager.
//     */
//    @Test
//    public void testRenew() throws Exception {
//        System.out.println("renew");
//        DocumentServiceLease lease = null;
//        DocumentServiceLeaseManager instance = null;
//        DocumentServiceLease expResult = null;
//        DocumentServiceLease result = instance.renew(lease);
//        assertEquals(expResult, result);
//        // TODO review the generated test code and remove the default call to fail.
//        fail("The test case is a prototype.");
//    }
//
//    /**
//     * Test of release method, of class DocumentServiceLeaseManager.
//     */
//    @Test
//    public void testRelease() throws Exception {
//        System.out.println("release");
//        DocumentServiceLease lease = null;
//        DocumentServiceLeaseManager instance = null;
//        boolean expResult = false;
//        boolean result = instance.release(lease);
//        assertEquals(expResult, result);
//        // TODO review the generated test code and remove the default call to fail.
//        fail("The test case is a prototype.");
//    }
//
//    /**
//     * Test of delete method, of class DocumentServiceLeaseManager.
//     */
//    @Test
//    public void testDelete() throws Exception {
//        System.out.println("delete");
//        DocumentServiceLease lease = null;
//        DocumentServiceLeaseManager instance = null;
//        instance.delete(lease);
//        // TODO review the generated test code and remove the default call to fail.
//        fail("The test case is a prototype.");
//    }
//
//    /**
//     * Test of deleteAll method, of class DocumentServiceLeaseManager.
//     */
//    @Test
//    public void testDeleteAll() throws Exception {
//        System.out.println("deleteAll");
//        DocumentServiceLeaseManager instance = null;
//        instance.deleteAll();
//        // TODO review the generated test code and remove the default call to fail.
//        fail("The test case is a prototype.");
//    }
//
//    /**
//     * Test of isExpired method, of class DocumentServiceLeaseManager.
//     */
//    @Test
//    public void testIsExpired() {
//        System.out.println("isExpired");
//        DocumentServiceLease lease = null;
//        DocumentServiceLeaseManager instance = null;
//        boolean expResult = false;
//        boolean result = instance.isExpired(lease);
//        assertEquals(expResult, result);
//        // TODO review the generated test code and remove the default call to fail.
//        fail("The test case is a prototype.");
//    }
//
//    /**
//     * Test of checkpoint method, of class DocumentServiceLeaseManager.
//     */
//    @Test
//    public void testCheckpoint() {
//        System.out.println("checkpoint");
//        Lease lease = null;
//        String continuationToken = "";
//        long sequenceNumber = 0L;
//        DocumentServiceLeaseManager instance = null;
//        Lease expResult = null;
//        Lease result = instance.checkpoint(lease, continuationToken, sequenceNumber);
//        assertEquals(expResult, result);
//        // TODO review the generated test code and remove the default call to fail.
//        fail("The test case is a prototype.");
//    }
//    
}
