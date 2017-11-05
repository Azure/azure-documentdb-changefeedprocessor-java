/**
 * The MIT License (MIT)
 * Copyright (c) 2016 Microsoft Corporation
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

import com.microsoft.azure.documentdb.*;
import com.microsoft.azure.documentdb.changefeedprocessor.DocumentCollectionInfo;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.*;
import com.microsoft.azure.documentdb.changefeedprocessor.services.DocumentServices;
import org.apache.http.HttpStatus;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author yoterada
 */
// CR: why is this class public? Should be internal. Or it this some sort of Java thing making all classes public?
// CR: also, there are lots of classes that are internal in C# are public in Java version.
public class DocumentServiceLeaseManager implements ILeaseManager<DocumentServiceLease>, ICheckpointManager {
    private final static String DATE_HEADER_NAME = "Date";
    private final static String CONTAINER_SEPARATOR = ".";
    private final static String PARTITION_PREFIX = ".";
    private final static String CONTAINER_NAME_SUFFIX = "info";
    private final static int RETRY_COUNT_ON_CONFLICT = 5;
    private String containerNamePrefix;
    private DocumentCollectionInfo leaseStoreCollectionInfo;
    private Duration leaseIntervalAllowance = Duration.ofMillis(25);
    private Duration leaseInterval;
    private Duration renewInterval;
    private DocumentServices documentServices;

    private String leaseStoreCollectionLink;
    private Duration serverToLocalTimeDelta;
    private Logger logger = Logger.getLogger(DocumentServiceLeaseManager.class.getName());

    @FunctionalInterface
    private interface LeaseConflictResolver {
        DocumentServiceLease run(DocumentServiceLease serverLease);
    }

    public DocumentServiceLeaseManager(DocumentCollectionInfo leaseStoreCollectionInfo, String storeNamePrefix, Duration leaseInterval, Duration renewInterval, DocumentServices documentServices) {
        this.leaseStoreCollectionInfo = leaseStoreCollectionInfo;
        this.containerNamePrefix = storeNamePrefix;
        this.leaseInterval = leaseInterval;
        this.renewInterval = renewInterval;
        this.documentServices = documentServices;
    }

    public void dispose() {
    }

    // CR: createLeaseColection = true has an issue that we don't know which collection to create (offerThroughput, etc)
    //     that's why in C# version we always use pre-created lease collection.
    //     Do we really need this scenario (true)?
    public void initialize(boolean createLeaseCollection) throws DocumentClientException {

        //Create URI String
        String uri = String.format("/dbs/%s/colls/%s", leaseStoreCollectionInfo.getDatabaseName(), leaseStoreCollectionInfo.getCollectionName());

        try {
            ResourceResponse response = documentServices.readCollection(uri, new RequestOptions());
            if (response != null)
                leaseStoreCollectionLink = response.getResource().getSelfLink();
        } catch (DocumentClientException ex) {
            if (createLeaseCollection && ex.getStatusCode() == 404 ) { //Collection Lease Not Found)
                logger.info("Parameter createLeaseCollection is true! Creating lease collection");

                DocumentCollection leaseColl = new DocumentCollection();
                leaseColl.setId(leaseStoreCollectionInfo.getCollectionName());

                ResourceResponse response = documentServices.createCollection(String.format("/dbs/%s", leaseStoreCollectionInfo.getDatabaseName()),leaseColl,new RequestOptions());
                leaseStoreCollectionLink = response.getResource().getSelfLink();

            } else {
                if (!createLeaseCollection)
                    logger.info("Parameter createLeaseCollection is false! Creating lease collection");
                throw ex;
            }
        }

        // Get the current time
        Instant snapshot1 = Instant.now();

        // Create and upload a new document
        Document document = new Document();
        document.setId(getDocumentId() + UUID.randomUUID().toString());

        // CR: move logic of getting time delta into separate method. We are going to have an option to disable this.
        Document dummyDocument = (Document)documentServices.createDocument(leaseStoreCollectionLink, document, new RequestOptions(), true).getResource();
        //Document dummyDocument = client.createDocument(leaseStoreCollectionLink, document, new RequestOptions(), true).getResource();

        // Get the new current time
        Instant snapshot2 = Instant.now();

        Instant dummyTimestamp = Instant.ofEpochSecond(dummyDocument.getTimestamp().getTime()); // Instant defaults to UTC
        Instant currentTimeDiff = Instant.ofEpochSecond(snapshot1.plusSeconds(snapshot2.getEpochSecond()).getEpochSecond() / 2);
        serverToLocalTimeDelta = Duration.between(currentTimeDiff, dummyTimestamp);

        documentServices.deleteDocument(dummyDocument.getSelfLink(), new RequestOptions());
       // client.deleteDocument(dummyDocument.getSelfLink(), new RequestOptions());

        logger.info(String.format("Server to local time delta: {0}", serverToLocalTimeDelta));
    }

    @Override
    public Callable<Boolean> leaseStoreExists() throws DocumentClientException {

        Callable<Boolean> callable = new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                //TODO: Fix with callable	// CR: is there still any issue to address?
                DocumentServiceLease containerDocument = tryGetLease(getDocumentId());
                return new Boolean(containerDocument != null);
            }
        };

        return callable;
    }

    @Override
    public Callable<Boolean> createLeaseStoreIfNotExists() throws DocumentClientException {

        Callable<Boolean> callable = new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                Boolean wasCreated = false;
                try {
                    if (!leaseStoreExists().call().booleanValue()) {
                        Document containerDocument = new Document();
                        containerDocument.setId(getDocumentId());

                        documentServices.createDocument(leaseStoreCollectionLink, containerDocument, new RequestOptions(), true);
                        wasCreated = true;
                    }
                } catch (Exception e) {
                }
                return wasCreated;
            }
        };

        return callable;
    }

    @Override
    public Callable<Iterable<DocumentServiceLease>> listLeases() {

        Callable<Iterable<DocumentServiceLease>> callable = new Callable<Iterable<DocumentServiceLease>>() {
            @Override
            public Iterable<DocumentServiceLease> call() throws Exception {
                return listDocuments(getPartitionLeasePrefix());
            }
        };

        return callable;
    }

    /**
     * Checks whether lease exists and creates if does not exist.
     * @return true if created, false otherwise. */
    @Override
    public Callable<Boolean> createLeaseIfNotExist(String partitionId, String continuationToken) throws DocumentClientException {

        Callable<Boolean> callable = new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                Boolean wasCreated = false;
                String leaseDocId = getDocumentId(partitionId);

                if (tryGetLease(leaseDocId) == null) {
                    DocumentServiceLease documentServiceLease = new DocumentServiceLease();
                    documentServiceLease.setId(leaseDocId);
                    documentServiceLease.setPartitionId(partitionId);
                    documentServiceLease.setContinuationToken(continuationToken);

                    documentServices.createDocument(leaseStoreCollectionLink, documentServiceLease, new RequestOptions(), true);
                    wasCreated = true;
                }
                return wasCreated;
            }
        };

        return callable;
    }

    @Override
    public Callable<DocumentServiceLease> getLease(String partitionId) throws DocumentClientException {

        Callable<DocumentServiceLease> callable = new Callable<DocumentServiceLease>() {
            @Override
            public DocumentServiceLease call() throws Exception {
                return tryGetLease(getDocumentId(partitionId));
            }
        };

        return callable;
    }

    @Override
    public Callable<DocumentServiceLease> acquire(DocumentServiceLease lease, String owner) throws DocumentClientException {

        if (lease == null || lease.getPartitionId() == null) {
            throw new IllegalArgumentException("lease");
        }

        if (owner == null || owner.isEmpty()) {
            throw new IllegalArgumentException("owner");
        }

        Callable<DocumentServiceLease> callable = new Callable<DocumentServiceLease>() {
            @Override
            public DocumentServiceLease call() throws Exception {
                DocumentServiceLease currentLease = tryGetLease(getDocumentId(lease.getPartitionId()));
                currentLease.setOwner(owner);
                currentLease.setState(LeaseState.LEASED);

                try {
                    return updateInternal(currentLease, (DocumentServiceLease serverLease) -> {
                        serverLease.setOwner(currentLease.getOwner());
                        serverLease.setState(currentLease.getState());
                        return serverLease;
                    }, owner);
                } catch (LeaseLostException | DocumentClientException ex) {
                    Logger.getLogger(DocumentServiceLeaseManager.class.getName()).log(Level.SEVERE, null, ex);	// CR: why eat exceptions?
                }
                return null;
            }
        };

        return callable;
    }

    @Override
    public Callable<DocumentServiceLease> renew(DocumentServiceLease lease) throws LeaseLostException, DocumentClientException {

        if (lease == null) throw new AssertionError("lease");

        Callable<DocumentServiceLease> callable = new Callable<DocumentServiceLease>() {
            @Override
            public DocumentServiceLease call() throws Exception {
                DocumentServiceLease refreshedLease = tryGetLease(getDocumentId(lease.getPartitionId()));
                if (refreshedLease == null)
                {
                	// CR: for consistency, should use this.logger, everywhere.
                    logger.info(String.format("Failed to renew lease for partition id %s! The lease is gone already.", lease.getPartitionId()));
                    throw new LeaseLostException(lease);
                }
                else if (refreshedLease.getOwner()!= null && !refreshedLease.getOwner().equals(lease.getOwner()))
                {
                    logger.info(String.format("Failed to renew lease for partition id $s! The lease was already taken by another host.", lease.getPartitionId()));
                    throw new LeaseLostException(lease);
                }
                return updateInternal(refreshedLease, (DocumentServiceLease serverLease) -> serverLease, null);
            }
        };

        return callable;
    }

    @Override
    public Callable<Boolean> release(DocumentServiceLease lease) throws DocumentClientException, LeaseLostException {

        Callable<Boolean> callable = new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                DocumentServiceLease refreshedLease = tryGetLease(getDocumentId(lease.getPartitionId()));
                if (refreshedLease == null) {
                    logger.info(String.format("Failed to release lease for partition id %s! The lease is gone already.", lease.getPartitionId()));
                    return false;
                } else if (!refreshedLease.getOwner().equals(lease.getOwner())) {
                    logger.info(String.format("No need to release lease for partition id %s! The lease was already taken by another host.", lease.getPartitionId()));
                    return true;
                } else {
                    String oldOwner = lease.getOwner();
                    refreshedLease.setOwner(null);
                    refreshedLease.setState(LeaseState.AVAILABLE);
                    refreshedLease = updateInternal(refreshedLease, (DocumentServiceLease serverLease) -> {
                        serverLease.setOwner(null); // In the lambda expression of Java, only access effective final value;
                        serverLease.setState(LeaseState.AVAILABLE); // In the lambda expression of Java, only access effective final value;
                        return serverLease;
                    }, oldOwner);
                    if (refreshedLease != null) {
                        return true;
                    } else {
                        logger.info(String.format("Failed to release lease for partition id {0}! Probably the lease was stolen by another host.", lease.getPartitionId()));
                        return false;
                    }
                }
            }
        };

        return callable;
    }

    @Override
    public Callable<Void> delete(DocumentServiceLease lease) throws DocumentClientException, LeaseLostException {
        if (lease == null || lease.getId() == null) {
            throw new IllegalArgumentException("lease");
        }

        Callable<Void> callable = new Callable<Void>() {
            @Override
            public Void call() throws Exception {

                //Create URI String
                String uri = String.format("/dbs/%s/colls/%s/docs/%s", leaseStoreCollectionInfo.getDatabaseName(), leaseStoreCollectionInfo.getCollectionName(), lease.getId());
                try {
                    documentServices.deleteDocument(uri, new RequestOptions());
                } catch (DocumentClientException ex) {
                    if (HttpStatus.SC_NOT_FOUND != ex.getStatusCode())
                    {
                        handleLeaseOperationException(lease, ex);
                    }
                }
                return null;
            }
        };

        return callable;
    }

    @Override
    public Callable<Void> deleteAll() throws DocumentClientException, LeaseLostException {

        Callable<Void> callable = new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                Iterable<DocumentServiceLease> listDocuments = listDocuments(getDocumentId(""));
                for (DocumentServiceLease lease : listDocuments) {
                    delete(lease).call();
                }
                return null;
            }
        };

        return callable;
    }

    @Override
    public Callable<Boolean> isExpired(DocumentServiceLease lease) {
        if ((lease == null)) throw new AssertionError();

        Callable<Boolean> callable = new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {

                // Lease time converted to seconds
                long leaseSeconds = lease.getTimestamp().getEpochSecond();

                // Current time converted to seconds
                long currentSeconds = Instant.now().getEpochSecond();

                Duration leaseExpiration = leaseInterval.plusSeconds(leaseSeconds).plus(leaseIntervalAllowance);
                Duration serverTime = serverToLocalTimeDelta.plusSeconds(currentSeconds);
                return leaseExpiration.getSeconds() < serverTime.getSeconds();
            }
        };

        return callable;
    }

    @Override
    public void createLeases(Hashtable<String, PartitionKeyRange> ranges){
        // Get leases after getting ranges, to make sure that no other hosts checked in continuation for split partition after we got leases.
        ConcurrentHashMap existingLeases = new ConcurrentHashMap<String, DocumentServiceLease>();
        try {
            listLeases().call().forEach((lease) -> {
                existingLeases.put(lease.getPartitionId(), lease);
            });
        } catch (Exception e) {
            logger.severe(e.getMessage());	// Why eat exceptions?
        }

        HashSet<String> gonePartitionIds = new HashSet<>();
        existingLeases.keySet().forEach((key) -> {
            String partitionID = (String)key;
            if(!ranges.contains(partitionID))
                gonePartitionIds.add(partitionID);
        });

        ArrayList<String> addedPartitionIds = new ArrayList<>();
        ranges.keySet().stream().forEach((range) -> {
            if (!existingLeases.containsKey(range))
                addedPartitionIds.add(range);
        });

        ConcurrentHashMap<String, ConcurrentLinkedQueue<DocumentServiceLease>> parentIdToChildLeases = new ConcurrentHashMap<>();

        addedPartitionIds.forEach((addedRangeId) -> {
            String continuationToken = null;
            String parentIds = "";

            PartitionKeyRange range = ranges.get(addedRangeId);
            if (range.getParents()!= null && range.getParents().size() > 0)   // Check for split.
            {
                for (String parentRangeId : range.getParents()){
                    if (gonePartitionIds.contains(parentRangeId))
                    {
                        // Transfer ContinuationToken from lease for gone parent to lease for its child partition.
                        parentIds += parentIds.length() == 0 ? parentRangeId : "," + parentRangeId;
                        DocumentServiceLease parentLease = (DocumentServiceLease)existingLeases.get(parentRangeId);
                        if (continuationToken != null)
                        {
                            logger.warning(String.format("Partition {0}: found more than one parent, new continuation '{1}', current '{2}', will use '{3}'", addedRangeId, parentLease.getContinuationToken(), continuationToken, parentLease.getContinuationToken()));
                        }
                        continuationToken = parentLease.getContinuationToken();
                    }
                }
            }

            try {
                if (continuationToken == null &&  existingLeases != null && existingLeases.get(addedRangeId) != null)
                    continuationToken = ((DocumentServiceLease)existingLeases.get(addedRangeId)).getContinuationToken();

                if (continuationToken == null)
                    continuationToken = "";

                createLeaseIfNotExist(addedRangeId, continuationToken).call();
            } catch (DocumentClientException e) {
                logger.severe(String.format("Error creating lease %s", e.getMessage()));	// CR: why eat exception?
            } catch (Exception e) {
                e.printStackTrace();														// CR: why eat exception?
            }
        });
        
        // CR: important: must remove gone leases (for every item in gonePartitionIds).
    }

    @Override
    public Lease checkpoint(Lease lease, String continuationToken, long sequenceNumber) {
        DocumentServiceLease documentLease = (DocumentServiceLease) lease;
        assert documentLease != null : "documentLease";
        try {
            documentLease.setContinuationToken(continuationToken);
            documentLease.setSequenceNumber(sequenceNumber);
            DocumentServiceLease result = updateInternal(documentLease, (DocumentServiceLease serverLease) -> {
                serverLease.setContinuationToken(continuationToken);
                serverLease.setSequenceNumber(sequenceNumber);
                return serverLease;
            }, DATE_HEADER_NAME);
            return result;
        } catch (LeaseLostException | DocumentClientException ex) {
            Logger.getLogger(DocumentServiceLeaseManager.class.getName()).log(Level.SEVERE, null, ex);	// CR: eating exceptions.
        }
        return null;
    }



    private Document tryGetDocument(String documentId) throws DocumentClientException {
        String uri = String.format("/dbs/%s/colls/%s/docs/%s", leaseStoreCollectionInfo.getDatabaseName(), leaseStoreCollectionInfo.getCollectionName(), documentId);
        logger.info(String.format("getting document uri %s", uri));
        Document doc = null;
        try {
            doc = documentServices.readDocument(uri, new RequestOptions()).getResource();
        } catch (DocumentClientException ex) {
        	if(HttpStatus.SC_NOT_FOUND != ex.getStatusCode()) {
        		throw ex;
        	}
        }
        
        return doc;
    }

    private DocumentServiceLease tryGetLease(String documentId) throws DocumentClientException {
        Document leaseDocument = tryGetDocument(documentId);
        if (leaseDocument != null) {
        	return new DocumentServiceLease(leaseDocument);
        } else {
            return null;
        }
    }

    private Iterable<DocumentServiceLease> listDocuments(String prefix) {//    private Task<IEnumerable<DocumentServiceLease>> ListDocuments(string prefix)
        assert prefix != null && !prefix.isEmpty() : "prefix";

        SqlParameter param = new SqlParameter();
        param.setName("@PartitionLeasePrefix");
        param.setValue(prefix);
        SqlQuerySpec querySpec = new SqlQuerySpec(
                String.format(Locale.ROOT, "SELECT * FROM c WHERE STARTSWITH(c.id, @PartitionLeasePrefix)"),
                new SqlParameterCollection(new SqlParameter[] { param }));

        FeedResponse<Document> queryResults = documentServices.queryDocuments(leaseStoreCollectionLink, querySpec, null);

        List<DocumentServiceLease> docs = new ArrayList<>();
        queryResults.getQueryIterable().forEach((Document d) -> {	
        	docs.add(new DocumentServiceLease(d));
        });
        
        return docs;
    }


    private String getDocumentId() {    
        return getDocumentId(null);
    }

    /**
     * Creates id either for container (if partitionId parameter is empty) or for lease otherwise.
     * @param partitionId, the lease partition id.
     * @return Document id for container or lease. */

    private String getDocumentId(String partitionId) {//    private string GetDocumentId(string partitionId = null)
        if (partitionId == null || partitionId.equals("")) {
            return containerNamePrefix + DocumentServiceLeaseManager.CONTAINER_SEPARATOR + DocumentServiceLeaseManager.CONTAINER_NAME_SUFFIX;
        } else {
            return getPartitionLeasePrefix() + partitionId;
        }
    }
    
    private String getPartitionLeasePrefix() {
        return containerNamePrefix + CONTAINER_SEPARATOR + CONTAINER_NAME_SUFFIX + PARTITION_PREFIX;
    }

    private DocumentServiceLease updateInternal(DocumentServiceLease lease, LeaseConflictResolver conflictResolver, String owner) 
    		throws LeaseLostException, DocumentClientException {
        assert lease != null : "lease";
        assert lease.getId() != null && !lease.getId().isEmpty() : "lease.Id";

        if (lease.getOwner() != null && !lease.getOwner().isEmpty()) {
            owner = lease.getOwner();
        }

        String leaseUri = String.format("/dbs/%s/colls/%s/docs/%s/", leaseStoreCollectionInfo.getDatabaseName(), leaseStoreCollectionInfo.getCollectionName(), lease.getId());
        int retryCount = RETRY_COUNT_ON_CONFLICT;
        while (true) {
            Document leaseDocument = null;
            try {
                leaseDocument = documentServices.replaceDocument(leaseUri, lease, createIfMatchOptions(lease)).getResource();
            } catch (DocumentClientException ex) {
                if (HttpStatus.SC_PRECONDITION_FAILED != ex.getStatusCode()) {
                    handleLeaseOperationException(lease, ex);

                    assert false : "UpdateInternalAsync: should never reach this!";
                    throw new LeaseLostException(lease);
                }
            }

            if (leaseDocument != null) {
                return new DocumentServiceLease(leaseDocument);
            } else {
                // Check if precondition failed due to a change from same/this host and retry.
                Document document = tryGetDocument(getDocumentId(lease.getPartitionId()));
                DocumentServiceLease serverLease = new DocumentServiceLease(document);
                if (!serverLease.getOwner().equals(owner)) {
                    throw new LeaseLostException(lease);
                }

                if (retryCount-- > 0) {
                    logger.info(String.format("Partition '{0}' update failed because the lease with token '{1}' was updated by same/this host with token '{2}'. Will retry, {3} retry(s) left.", lease.getPartitionId(), lease.getConcurrencyToken(), serverLease.getConcurrencyToken(), retryCount));

                    lease = conflictResolver.run(serverLease);
                } else {
                    throw new LeaseLostException(lease);
                }
            }
        }
    }

    private RequestOptions createIfMatchOptions(DocumentServiceLease lease) {
        assert lease != null : "lease";

        AccessCondition ifMatchCondition = new AccessCondition();
        ifMatchCondition.setType(AccessConditionType.IfMatch);
        ifMatchCondition.setCondition(lease.eTag);

        RequestOptions options = new RequestOptions();
        options.setAccessCondition(ifMatchCondition);

        return options;
    }

    private void handleLeaseOperationException(DocumentServiceLease lease, DocumentClientException dcex) throws DocumentClientException, LeaseLostException {
        assert lease != null : "lease";
        assert dcex != null : "dispatchInfo";

        logger.warning(String.format("Lease operation exception, status code: ", dcex.getStatusCode()));

        if (HttpStatus.SC_PRECONDITION_FAILED == dcex.getStatusCode()
                || HttpStatus.SC_CONFLICT == dcex.getStatusCode()
                || HttpStatus.SC_NOT_FOUND == dcex.getStatusCode()) {
            throw new LeaseLostException(lease, dcex, HttpStatus.SC_NOT_FOUND == dcex.getStatusCode());
        } else {
            throw dcex;
        }
    }
}

class PartitionInfo implements Partition {	// CR: move to separate file. Is this actually used anywhere? If not, let's remove.

    public String Etag;
    public String DatabaseName;
    public String CollName;
    public String ID;
    public PartitionStatus Status;
    public Date LastExecution;

    public PartitionInfo() {
        Etag = null;
        DatabaseName = null;
        CollName = null;
        ID = null;
        Status = PartitionStatus.SYNCING;
    }

    public PartitionInfo(String databaseName, String collName, String id, String partitionEtag, PartitionStatus status) {
        this.Etag = partitionEtag;
        this.DatabaseName = databaseName;
        this.CollName = collName;
        this.ID = id;
        this.Status = status;
        this.LastExecution = null;
    }

    @Override
    public Date lastExcetution() {
        return this.LastExecution;
    }

    @Override
    public void updateExecution() {
        this.LastExecution = new Date(System.currentTimeMillis());
    }

    @Override
    public String key() {
        return String.format("%s,%s,%s", this.DatabaseName, this.CollName, this.ID);
    }

    public enum PartitionStatus {
        COMPLETED, SYNCING;
    }
}

interface Partition {	// CR: move to separate file

    public Date lastExcetution();

    public void updateExecution();

    public String key();
}
