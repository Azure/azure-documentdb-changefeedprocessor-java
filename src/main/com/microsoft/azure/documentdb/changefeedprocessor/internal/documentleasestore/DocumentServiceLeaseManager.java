/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.microsoft.azure.documentdb.changefeedprocessor.internal.documentleasestore;

import com.microsoft.azure.documentdb.AccessCondition;
import com.microsoft.azure.documentdb.AccessConditionType;
import com.microsoft.azure.documentdb.ChangeFeedOptions;
import com.microsoft.azure.documentdb.ConsistencyLevel;
import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.DocumentCollection;
import com.microsoft.azure.documentdb.FeedOptions;
import com.microsoft.azure.documentdb.FeedResponse;
import com.microsoft.azure.documentdb.RequestOptions;
import com.microsoft.azure.documentdb.ResourceResponse;
import com.microsoft.azure.documentdb.SqlParameter;
import com.microsoft.azure.documentdb.SqlParameterCollection;
import com.microsoft.azure.documentdb.SqlQuerySpec;
import com.microsoft.azure.documentdb.changefeedprocessor.DocumentCollectionInfo;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.ICheckpointManager;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.ILeaseManager;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.Lease;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.LeaseLostException;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.TraceLog;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoField;
import java.util.Date;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.http.HttpStatus;

/**
 *
 * @author yoterada
 */
public class DocumentServiceLeaseManager implements ILeaseManager<DocumentServiceLease>, ICheckpointManager {

    private final static Logger LOGGER = Logger.getLogger(DocumentServiceLeaseManager.class.getName());

    private final static String dateHeaderName = "Date";
    private final static String containerSeparator = ".";
    private final static String partitionPrefix = ".";
    private final static String containerNameSuffix = "info";
    private final static int retryCountOnConflict = 5;
    private String containerNamePrefix;
    private DocumentCollectionInfo leaseStoreCollectionInfo;
//    private TimeSpan leaseIntervalAllowance = TimeSpan.FromMilliseconds(25);  // Account for diff between local and server.
    private Duration leaseIntervalAllowance = Duration.ofMillis(25);
    private Instant leaseInterval;
    private Instant renewInterval;

    private String leaseStoreCollectionLink;
    private Duration serverToLocalTimeDelta;

    DocumentClient client;

    @FunctionalInterface
    private interface LeaseConflictResolver {
    	DocumentServiceLease  run(DocumentServiceLease serverLease);
    }

    public DocumentServiceLeaseManager(DocumentCollectionInfo leaseStoreCollectionInfo, String storeNamePrefix, Instant leaseInterval, Instant renewInterval) {
        this.leaseStoreCollectionInfo = leaseStoreCollectionInfo;
        this.containerNamePrefix = storeNamePrefix;
        this.leaseInterval = leaseInterval;
        this.renewInterval = renewInterval;
        this.client = new DocumentClient(leaseStoreCollectionInfo.getUri().toString(), leaseStoreCollectionInfo.getMasterKey(), leaseStoreCollectionInfo.getConnectionPolicy(), ConsistencyLevel.Session);
    }

    public void dispose() {
    }

    public void initialize() throws DocumentClientException //    public Task InitializeAsync()
    {
        //Create URI String
        String uri = String.format("/dbs/%s/colls/%s", leaseStoreCollectionInfo.getDatabaseName(), leaseStoreCollectionInfo.getCollectionName());

        FeedOptions options = new FeedOptions();
        //TODO : we need the confirmation when we test the options.
        client.readDocuments(uri, options);

        Instant snapshot1 = Instant.now();

        //TODO: Test is needed
        Document document = new Document();
        document.setId(getDocumentId() + UUID.randomUUID().toString());
        //final boolean is "disableAutomaticIdGeneration - the flag for disabling automatic id generation."
        Document dummyDocument = client.createDocument(uri, document, new RequestOptions(), true).getResource();

        Instant snapshot2 = Instant.now();
        Duration between = Duration.between(snapshot1, snapshot2);

        Instant dummyTimestamp = dummyDocument.getTimestamp().toInstant();
        int nanovalue = snapshot1.getNano() + snapshot2.getNano() / 2;
        serverToLocalTimeDelta
                = Duration.between(dummyTimestamp, Instant.ofEpochMilli(nanovalue));
        client.deleteDocument(dummyDocument.getSelfLink(), new RequestOptions());

        LOGGER.fine(String.format("Server to local time delta: {0}", this.serverToLocalTimeDelta));

    }

    @Override
    public boolean leaseStoreExists() //    public async Task<bool> LeaseStoreExistsAsync()
    {
        DocumentServiceLease containerDocument = tryGetLease(getDocumentId());
        return containerDocument != null;
    }

    @Override
    public boolean createLeaseStoreIfNotExists() throws DocumentClientException { //    public  Task<bool> CreateLeaseStoreIfNotExistsAsync()

        boolean wasCreated = false;
        if (leaseStoreExists()) {
            Document containerDocumentnew = new Document();
            containerDocumentnew.setId(getDocumentId());

            client.createDocument(leaseStoreCollectionLink, containerDocumentnew, new RequestOptions(), true);
            wasCreated = true;
        }
        return wasCreated;
    }

    @Override
    public Iterable<DocumentServiceLease> listLeases() //    public Task<IEnumerable<DocumentServiceLease>> ListLeases()
    {
        return listDocuments(getPartitionLeasePrefix());
    }

    /// <summary>
    /// Checks whether lease exists and creates if does not exist.
    /// </summary>
    /// <returns>true if created, false otherwise.</returns>
    public boolean createLeaseIfNotExistAsync(String partitionId, String continuationToken) throws DocumentClientException //    public async Task<bool> CreateLeaseIfNotExistAsync(string partitionId, string continuationToken)
    {
        boolean wasCreated = false;
        String leaseDocId = getDocumentId(partitionId);

        if (tryGetLease(leaseDocId) == null) {
            DocumentServiceLease documentServiceLease = new DocumentServiceLease();
            documentServiceLease.setId(leaseDocId);
            documentServiceLease.setPartitionId(partitionId);
            documentServiceLease.setContinuationToken(continuationToken);

            client.createDocument(leaseStoreCollectionLink, documentServiceLease, new RequestOptions(), true);
            wasCreated = true;
        }
        return wasCreated;
    }

    public DocumentServiceLease getLeaseAsync(String partitionId) //    public async Task<DocumentServiceLease> GetLeaseAsync(string partitionId)
    {
        return tryGetLease(getDocumentId(partitionId));
    }

    public  DocumentServiceLease acquire(DocumentServiceLease lease, String owner) {//    public async Task<DocumentServiceLease> AcquireAsync(DocumentServiceLease lease, string owner)
    
        if (lease == null || lease.getPartitionId() == null)
        {
            throw new IllegalArgumentException("lease");
        }
        
        if (owner == null || owner.equals(""))
        {
            throw new IllegalArgumentException("owner");
        }
/*
        DocumentServiceLease currentLease = await this.TryGetLease(this.GetDocumentId(lease.PartitionId));
        currentLease.Owner = owner;
        currentLease.State = LeaseState.Leased;

        return await this.UpdateInternalAsync(
            currentLease,
            (DocumentServiceLease serverLease) =>
        {
            serverLease.Owner = currentLease.Owner;
            serverLease.State = currentLease.State;
            return serverLease;
        });*/
        return null;
    }

    public DocumentServiceLease renew(DocumentServiceLease lease) throws LeaseLostException, DocumentClientException {  //    public async Task<DocumentServiceLease> RenewAsync(DocumentServiceLease lease)
        assert lease != null : "lease";

        DocumentServiceLease refreshedLease = tryGetLease(getDocumentId(lease.getPartitionId()));
        if (refreshedLease == null)
        {
            TraceLog.informational(String.format("Failed to renew lease for partition id {0}! The lease is gone already.", lease.getPartitionId()));
            throw new LeaseLostException(lease);
        }
        else if (refreshedLease.getOwner() != lease.getOwner())
        {
            TraceLog.informational(String.format("Failed to renew lease for partition id {0}! The lease was already taken by another host.", lease.getPartitionId()));
            throw new LeaseLostException(lease);
        }
        return updateInternal(refreshedLease, serverLease -> serverLease, null);
    }

    public Future<V> releaseAsync(DocumentServiceLease lease) //    public async Task<bool> ReleaseAsync(DocumentServiceLease lease)
    {
        /*
        DocumentServiceLease refreshedLease = await this.TryGetLease(this.GetDocumentId(lease.PartitionId));
        if (refreshedLease == null)
        {
            TraceLog.Informational(string.Format("Failed to release lease for partition id {0}! The lease is gone already.", lease.PartitionId));
            return false;
        }
        else if (refreshedLease.Owner != lease.Owner)
        {
            TraceLog.Informational(string.Format("No need to release lease for partition id {0}! The lease was already taken by another host.", lease.PartitionId));
            return true;
        }

        string oldOwner = lease.Owner;
        refreshedLease.Owner = null;
        refreshedLease.State = LeaseState.Available;

        refreshedLease = await this.UpdateInternalAsync(
            refreshedLease,
            (DocumentServiceLease serverLease) =>
        {
            serverLease.Owner = refreshedLease.Owner;
            serverLease.State = refreshedLease.State;
            return serverLease;
        },
        oldOwner);
        if (refreshedLease != null)
        {
            return true;
        }
        else
        {
            TraceLog.Informational(string.Format("Failed to release lease for partition id {0}! Probably the lease was stolen by another host.", lease.PartitionId));
            return false;
        }
         */
    }

    public Future<V> deleteAsync(DocumentServiceLease lease) //    public async Task DeleteAsync(DocumentServiceLease lease)
    {
        /*
        if (lease == null || lease.Id == null)
        {
            throw new ArgumentException("lease");
        }

        Uri leaseUri = UriFactory.CreateDocumentUri(this.leaseStoreCollectionInfo.DatabaseName, this.leaseStoreCollectionInfo.CollectionName, lease.Id);
        try
        {
            await this.client.DeleteDocumentAsync(leaseUri);
        }
        catch (DocumentClientException ex)
        {
            if (StatusCode.NotFound != (StatusCode)ex.StatusCode)
            {
                this.HandleLeaseOperationException(lease, ExceptionDispatchInfo.Capture(ex));
            }
        }*/
    }

    public Future<V> deleteAllAsync() //    public async Task DeleteAllAsync()
    {
        /*
        var docs = await this.ListDocuments(this.containerNamePrefix);
        foreach (var doc in docs)
        {
            DocumentServiceLease lease = new DocumentServiceLease(doc);
            await this.DeleteAsync(lease);
        }*/
        return null;
    }

    public Future<V> isExpired(DocumentServiceLease lease) //    public Task<bool> IsExpired(DocumentServiceLease lease)
    {
        /*
        Debug.Assert(lease != null);

        return Task.FromResult<bool>(lease.Timestamp.ToUniversalTime() + this.leaseInterval + this.leaseIntervalAllowance < DateTime.UtcNow + this.serverToLocalTimeDelta);
         */
        return null;
    }

    public Future<V> checkpointAsync(Lease lease, string continuationToken, long sequenceNumber) //    public async Task<Lease> CheckpointAsync(Lease lease, string continuationToken, long sequenceNumber)
    {
        /*
        DocumentServiceLease documentLease = lease as DocumentServiceLease;
        Debug.Assert(documentLease != null, "documentLease");

        documentLease.ContinuationToken = continuationToken;
        documentLease.SequenceNumber = sequenceNumber;

        DocumentServiceLease result = await this.UpdateInternalAsync(
            documentLease,
            (DocumentServiceLease serverLease) =>
        {
            serverLease.ContinuationToken = documentLease.ContinuationToken;
            serverLease.SequenceNumber = documentLease.SequenceNumber;
            return serverLease;
        });

        return result;
         */
        return null;
    }

//    private async Task<DocumentServiceLease> UpdateInternalAsync(
    private Future<V> updateInternalAsync(
            DocumentServiceLease lease,
            LeaseConflictResolver conflictResolver,
            String owner) {
        /*
        Debug.Assert(lease != null, "lease");
        Debug.Assert(!string.IsNullOrEmpty(lease.Id), "lease.Id");

        if (string.IsNullOrEmpty(owner))
        {
            owner = lease.Owner;
        }

        Uri leaseUri = UriFactory.CreateDocumentUri(this.leaseStoreCollectionInfo.DatabaseName, this.leaseStoreCollectionInfo.CollectionName, lease.Id);
        int retryCount = RetryCountOnConflict;
        while (true)
        {
            Document leaseDocument = null;
            try
            {
                leaseDocument = await this.client.ReplaceDocumentAsync(leaseUri, lease, this.CreateIfMatchOptions(lease));
            }
            catch (DocumentClientException ex)
            {
                if (StatusCode.PreconditionFailed != (StatusCode)ex.StatusCode)
                {
                    ExceptionDispatchInfo.Capture(ex);
                    this.HandleLeaseOperationException(lease, ExceptionDispatchInfo.Capture(ex));

                    Debug.Assert(false, "UpdateInternalAsync: should never reach this!");
                    throw new LeaseLostException(lease);
                }
            }

            if (leaseDocument != null)
            {
                return new DocumentServiceLease(leaseDocument);
            }
            else
            {
                // Check if precondition failed due to a change from same/this host and retry.
                var document = await this.TryGetDocument(this.GetDocumentId(lease.PartitionId));
                var serverLease = new DocumentServiceLease(document);
                if (serverLease.Owner != owner)
                {
                    throw new LeaseLostException(lease);
                }

                if (retryCount-- > 0)
                {
                    TraceLog.Informational(string.Format("Partition '{0}' update failed because the lease with token '{1}' was updated by same/this host with token '{2}'. Will retry, {3} retry(s) left.", lease.PartitionId, lease.ConcurrencyToken, serverLease.ConcurrencyToken, retryCount));

                    lease = conflictResolver.run(serverLease);
                }
                else
                {
                    throw new LeaseLostException(lease);
                }
            }
        }
         */
        return null;
    }

    private Document tryGetDocument(String documentId) //    private async Task<Document> TryGetDocument(string documentId)
    {
        /*
        Uri documentUri = UriFactory.CreateDocumentUri(
                this.leaseStoreCollectionInfo.DatabaseName,
                this.leaseStoreCollectionInfo.CollectionName,
                documentId);

        Document document = null;
        try
        {
            document = await this.client.ReadDocumentAsync(documentUri);
        }
        catch (DocumentClientException ex)
        {
            if (StatusCode.NotFound != (StatusCode)ex.StatusCode)
            {
                throw;
            }
        }

        return document;
         */
        return null;
    }

    private DocumentServiceLease tryGetLease(String documentId) //    private async Task<DocumentServiceLease> TryGetLease(string documentId)
    {
        Document leaseDocument = tryGetDocument(documentId);
        if (leaseDocument != null) {
            return new DocumentServiceLease(leaseDocument);
        } else {
            return null;
        }
    }

    private Iterable<DocumentServiceLease> listDocuments(String prefix) //    private Task<IEnumerable<DocumentServiceLease>> ListDocuments(string prefix)
    {
        assert prefix != null && !prefix.isEmpty() : "prefix";

        SqlParameter param = new SqlParameter();
        param.setName("@PartitionLeasePrefix");
        param.setValue(prefix);
        SqlQuerySpec querySpec = new SqlQuerySpec(
                String.format(Locale.ROOT, "SELECT * FROM c WHERE STARTSWITH(c.id, @PartitionLeasePrefix)"),
                new SqlParameterCollection(new SqlParameter[] { param }));
        FeedResponse<Document> query = client.queryDocuments(leaseStoreCollectionLink, querySpec, new FeedOptions()); // createDocumentQuery<DocumentServiceLease>(this.leaseStoreCollectionLink, querySpec);

        return query;
    }

    /**
     * Creates id either for container (if partitionId parameter is empty) or for lease otherwise.
     * @param partitionId, the lease partition id.
     * @return Document id for container or lease. */
    private String getDocumentId() { //    private string GetDocumentId(string partitionId = null)    
        return getDocumentId(null);
    }

    private String getDocumentId(String partitionId) //    private string GetDocumentId(string partitionId = null)
    {
        if (partitionId == null || partitionId.equals("")) {
            return containerNamePrefix + DocumentServiceLeaseManager.containerSeparator + DocumentServiceLeaseManager.containerNameSuffix;
        } else {
            return getPartitionLeasePrefix() + partitionId;
        }
        /*
        return string.IsNullOrEmpty(partitionId) ?
                this.containerNamePrefix + DocumentServiceLeaseManager.ContainerSeparator + DocumentServiceLeaseManager.ContainerNameSuffix :
                this.GetPartitionLeasePrefix() + partitionId;
         */
    }
    
    private String getPartitionLeasePrefix() {
        return this.containerNamePrefix + containerSeparator + partitionPrefix;
    }

    public boolean LeaseStoreExistsAsync() //    public async Task<bool> LeaseStoreExistsAsync()
    {
        DocumentServiceLease containerDocument = tryGetLease(getDocumentId());
        return containerDocument != null ? true : false;
    }
 
    private DocumentServiceLease updateInternal(
            DocumentServiceLease lease,
            LeaseConflictResolver conflictResolver,
            String owner) throws LeaseLostException, DocumentClientException {
    	assert lease != null : "lease";
        assert lease.getId() != null && !lease.getId().isEmpty() : "lease.Id";

        if (lease.getOwner() != null && !lease.getOwner().isEmpty()) {
            owner = lease.getOwner();
        }

        String leaseUri = String.format("/dbs/%s/colls/%s/docs/%s/", leaseStoreCollectionInfo.getDatabaseName(), leaseStoreCollectionInfo.getCollectionName(), lease.getId());
        int retryCount = retryCountOnConflict;
        while (true) {
            Document leaseDocument = null;
            try {
                leaseDocument = client.replaceDocument(leaseUri, lease, createIfMatchOptions(lease)).getResource();
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
                if (serverLease.getOwner() != owner) {
                    throw new LeaseLostException(lease);
                }

                if (retryCount-- > 0) {
                    TraceLog.informational(String.format("Partition '{0}' update failed because the lease with token '{1}' was updated by same/this host with token '{2}'. Will retry, {3} retry(s) left.", lease.getPartitionId(), lease.getConcurrencyToken(), serverLease.getConcurrencyToken(), retryCount));

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

    private void handleLeaseOperationException(DocumentServiceLease lease, DocumentClientException dcex) throws DocumentClientException, LeaseLostException{
        assert lease != null : "lease";
        assert dcex != null : "dispatchInfo";

        TraceLog.warning(String.format("Lease operation exception, status code: ", dcex.getStatusCode()));

        if (HttpStatus.SC_PRECONDITION_FAILED == dcex.getStatusCode() ||
                HttpStatus.SC_CONFLICT == dcex.getStatusCode() ||
                HttpStatus.SC_NOT_FOUND == dcex.getStatusCode())
        {
            throw new LeaseLostException(lease, dcex, HttpStatus.SC_NOT_FOUND == dcex.getStatusCode());
        }
        else
        {
            throw dcex;
        }
    }
}

class PartitionInfo implements Partition {

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

interface Partition {

    public Date lastExcetution();

    public void updateExecution();

    public String key();
}
