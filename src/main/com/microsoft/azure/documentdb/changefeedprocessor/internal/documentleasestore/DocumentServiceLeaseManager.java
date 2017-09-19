/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.microsoft.azure.documentdb.changefeedprocessor.internal.documentLeaseStore;

import com.microsoft.azure.documentdb.changefeedprocessor.DocumentCollectionInfo;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.ICheckpointManager;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.ILeaseManager;

/**
 *
 * @author yoterada
 */
public class DocumentServiceLeaseManager implements ILeaseManager,ICheckpointManager {

    private final static String DateHeaderName = "Date";
    private final static String  ContainerSeparator = ".";
    private final static String  PartitionPrefix = ".";
    private final static String  ContainerNameSuffix = "info";
    private final static int RetryCountOnConflict = 5;
    private String containerNamePrefix;
    private DocumentCollectionInfo leaseStoreCollectionInfo;
    private TimeSpan leaseIntervalAllowance = TimeSpan.FromMilliseconds(25);  // Account for diff between local and server.
    private TimeSpan leaseInterval;
    private TimeSpan renewInterval;
    private String leaseStoreCollectionLink;
    Private TimeSpan serverToLocalTimeDelta;
    private delegate DocumentServiceLease LeaseConflictResolver(DocumentServiceLease serverLease);


    public DocumentServiceLeaseManager(DocumentCollectionInfo leaseStoreCollectionInfo, String storeNamePrefix, TimeSpan leaseInterval, TimeSpan renewInterval)
    {
        this.leaseStoreCollectionInfo = leaseStoreCollectionInfo;
        this.containerNamePrefix = storeNamePrefix;
        this.leaseInterval = leaseInterval;
        this.renewInterval = renewInterval;
//        this.client = new DocumentClient(leaseStoreCollectionInfo.Uri, leaseStoreCollectionInfo.MasterKey, leaseStoreCollectionInfo.ConnectionPolicy);
    }

    public void Dispose()
    {
    }

    public as    private final static String DateHeaderName = "Date";
    private final static String  ContainerSeparator = ".";
    private final static String  PartitionPrefix = ".";
    private final static String  ContainerNameSuffix = "info";
    private final static int RetryCountOnConflict = 5;
    private String containerNamePrefix;
    private DocumentCollectionInfo leaseStoreCollectionInfo;
    private TimeSpan leaseIntervalAllowance = TimeSpan.FromMilliseconds(25);  // Account for diff between local and server.
    private TimeSpan leaseInterval;
    private TimeSpan renewInterval;
    private String leaseStoreCollectionLink;
    Private TimeSpan serverToLocalTimeDelta;
    private delegate DocumentServiceLease LeaseConflictResolver(DocumentServiceLease serverLease);


    public DocumentServiceLeaseManager(DocumentCollectionInfo leaseStoreCollectionInfo, String storeNamePrefix, TimeSpan leaseInterval, TimeSpan renewInterval)
    {
        this.leaseStoreCollectionInfo = leaseStoreCollectionInfo;
        this.containerNamePrefix = storeNamePrefix;
        this.leaseInterval = leaseInterval;
        this.renewInterval = renewInterval;
//        this.client = new DocumentClient(leaseStoreCollectionInfo.Uri, leaseStoreCollectionInfo.MasterKey, leaseStoreCollectionInfo.ConnectionPolicy);
    }

    public void Dispose()
    {
    }

    public Future<V>  InitializeAsync()
//    public async Task InitializeAsync()
    {
        /*
        Uri collectionUri = UriFactory.CreateDocumentCollectionUri(this.leaseStoreCollectionInfo.DatabaseName, this.leaseStoreCollectionInfo.CollectionName);
        DocumentCollection collection = await this.client.ReadDocumentCollectionAsync(collectionUri);
        this.leaseStoreCollectionLink = collection.SelfLink;

        // Get delta between server and local time for time synchonization.
        DateTime snapshot1 = DateTime.UtcNow;
        Document dummyDocument = await this.client.CreateDocumentAsync(this.leaseStoreCollectionLink, new Document { Id = this.GetDocumentId() + Guid.NewGuid().ToString() });
        DateTime snapshot2 = DateTime.UtcNow;
        this.serverToLocalTimeDelta = dummyDocument.Timestamp.ToUniversalTime() - new DateTime((snapshot1.Ticks + snapshot2.Ticks) / 2, DateTimeKind.Utc);
        await this.client.DeleteDocumentAsync(dummyDocument.SelfLink);

        TraceLog.Verbose(string.Format("Server to local time delta: {0}", this.serverToLocalTimeDelta));
        */
    }

    public Future<V>  LeaseStoreExistsAsync()
//    public async Task<bool> LeaseStoreExistsAsync()
    {
        /*
        var containerDocument = await this.TryGetLease(this.GetDocumentId());
        return containerDocument != null ? true : false;
        */
    }

    public Future<V> CreateLeaseStoreIfNotExistsAsync()
//    public async Task<bool> CreateLeaseStoreIfNotExistsAsync()
    {
        /*
        bool wasCreated = false;

        if (!await this.LeaseStoreExistsAsync())
        {
            var containerDocument = new Document { Id = this.GetDocumentId() };

            try
            {
                await this.client.CreateDocumentAsync(this.leaseStoreCollectionLink, containerDocument);
                wasCreated = true;
            }
            catch (DocumentClientException ex)
            {
                if (StatusCode.Conflict != (StatusCode)ex.StatusCode)
                {
                    throw;
                }
            }
        }
        return wasCreated;
        */
        return null;
    }

    public Future<V> ListLeases()
//    public Task<IEnumerable<DocumentServiceLease>> ListLeases()
    {
//        return this.ListDocuments(this.GetPartitionLeasePrefix());
        return null;
    }

    /// <summary>
    /// Checks whether lease exists and creates if does not exist.
    /// </summary>
    /// <returns>true if created, false otherwise.</returns>
    public Future<V> CreateLeaseIfNotExistAsync(String partitionId, String continuationToken)
//    public async Task<bool> CreateLeaseIfNotExistAsync(string partitionId, string continuationToken)
    {
        /*
        bool wasCreated = false;
        var leaseDocId = this.GetDocumentId(partitionId);
        if (await this.TryGetLease(leaseDocId) == null)
        {
            try
            {
                await this.client.CreateDocumentAsync(
                    this.leaseStoreCollectionLink,
                    new DocumentServiceLease { Id = leaseDocId, PartitionId = partitionId, ContinuationToken = continuationToken });
                wasCreated = true;
            }
            catch (DocumentClientException ex)
            {
                if (StatusCode.Conflict != (StatusCode)ex.StatusCode)
                {
                    throw;
                }
            }
        }
        return wasCreated;
        */
        return null;
    }

    public  Future<V> GetLeaseAsync(string partitionId)
//    public async Task<DocumentServiceLease> GetLeaseAsync(string partitionId)
    {
//        return await this.TryGetLease(this.GetDocumentId(partitionId));
        return null;
    }

    public Future<V> AcquireAsync(DocumentServiceLease lease, string owner)
//    public async Task<DocumentServiceLease> AcquireAsync(DocumentServiceLease lease, string owner)
    {
        /*
        if (lease == null || lease.PartitionId == null)
        {
            throw new ArgumentException("lease");
        }

        if (string.IsNullOrWhiteSpace(owner))
        {
            throw new ArgumentException("owner");
        }

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
    }

    public Future<V> RenewAsync(DocumentServiceLease lease)
//    public async Task<DocumentServiceLease> RenewAsync(DocumentServiceLease lease)
    {
        /*
        Debug.Assert(lease != null, "lease");

        DocumentServiceLease refreshedLease = await this.TryGetLease(this.GetDocumentId(lease.PartitionId));
        if (refreshedLease == null)
        {
            TraceLog.Informational(string.Format("Failed to renew lease for partition id {0}! The lease is gone already.", lease.PartitionId));
            throw new LeaseLostException(lease);
        }
        else if (refreshedLease.Owner != lease.Owner)
        {
            TraceLog.Informational(string.Format("Failed to renew lease for partition id {0}! The lease was already taken by another host.", lease.PartitionId));
            throw new LeaseLostException(lease);
        }
        return await this.UpdateInternalAsync(refreshedLease, serverLease => serverLease);
         */
        return null;
    }

    public Future<V> ReleaseAsync(DocumentServiceLease lease)
//    public async Task<bool> ReleaseAsync(DocumentServiceLease lease)
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

    public Future<V> DeleteAsync(DocumentServiceLease lease)
//    public async Task DeleteAsync(DocumentServiceLease lease)
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

    public Future<V>  DeleteAllAsync()
//    public async Task DeleteAllAsync()
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

    public Future<V> IsExpired(DocumentServiceLease lease)
    //    public Task<bool> IsExpired(DocumentServiceLease lease)
    {
        /*
        Debug.Assert(lease != null);

        return Task.FromResult<bool>(lease.Timestamp.ToUniversalTime() + this.leaseInterval + this.leaseIntervalAllowance < DateTime.UtcNow + this.serverToLocalTimeDelta);
        */
        return null;
    }

    public Future<V> CheckpointAsync(Lease lease, string continuationToken, long sequenceNumber)
//    public async Task<Lease> CheckpointAsync(Lease lease, string continuationToken, long sequenceNumber)
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
private Future<V> UpdateInternalAsync(
            DocumentServiceLease lease,
            LeaseConflictResolver conflictResolver,
            string owner = null)
    {
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

                    lease = conflictResolver(serverLease);
                }
                else
                {
                    throw new LeaseLostException(lease);
                }
            }
        }
        */
        return  null;

    }

    private async Task<Document> TryGetDocument(string documentId)
    {
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
    }



    private async Task<DocumentServiceLease> TryGetLease(string documentId)
    {
        Document leaseDocument = await this.TryGetDocument(documentId);

        if (leaseDocument != null)
        {
            return new DocumentServiceLease(leaseDocument);
        }
        else
        {
            return null;
        }
    }

    private Task<IEnumerable<DocumentServiceLease>> ListDocuments(string prefix)
    {
        Debug.Assert(!string.IsNullOrEmpty(prefix), "prefix");

        var querySpec = new SqlQuerySpec(
                string.Format(CultureInfo.InvariantCulture, "SELECT * FROM c WHERE STARTSWITH(c.id, @PartitionLeasePrefix)"),
                new SqlParameterCollection(new SqlParameter[] { new SqlParameter { Name = "@PartitionLeasePrefix", Value = prefix } }));
        var query = this.client.CreateDocumentQuery<DocumentServiceLease>(this.leaseStoreCollectionLink, querySpec);

        return Task.FromResult<IEnumerable<DocumentServiceLease>>(query.AsEnumerable<DocumentServiceLease>());
    }

    /// <summary>
    /// Creates id either for container (if partitionId parameter is empty) or for lease otherwise.
    /// </summary>
    /// <param name="partitionId">The lease partition id.</param>
    /// <returns>Document id for container or lease.</returns>
    private string GetDocumentId(string partitionId = null)
    {
        return string.IsNullOrEmpty(partitionId) ?
                this.containerNamePrefix + DocumentServiceLeaseManager.ContainerSeparator + DocumentServiceLeaseManager.ContainerNameSuffix :
                this.GetPartitionLeasePrefix() + partitionId;
    }

    private RequestOptions CreateIfMatchOptions(DocumentServiceLease lease)
    {
        Debug.Assert(lease != null, "lease");

        AccessCondition ifMatchCondition = new AccessCondition { Type = AccessConditionType.IfMatch, Condition = lease.ETag };
        return new RequestOptions { AccessCondition = ifMatchCondition };
    }

    private void HandleLeaseOperationException(DocumentServiceLease lease, ExceptionDispatchInfo dispatchInfo)
    {
        Debug.Assert(lease != null, "lease");
        Debug.Assert(dispatchInfo != null, "dispatchInfo");

        DocumentClientException dcex = (DocumentClientException)dispatchInfo.SourceException;
        TraceLog.Warning(string.Format("Lease operation exception, status code: ", dcex.StatusCode));

        if (StatusCode.PreconditionFailed == (StatusCode)dcex.StatusCode ||
                StatusCode.Conflict == (StatusCode)dcex.StatusCode ||
                StatusCode.NotFound == (StatusCode)dcex.StatusCode)
        {
            throw new LeaseLostException(lease, dcex, StatusCode.NotFound == (StatusCode)dcex.StatusCode);
        }
        else
        {
            dispatchInfo.Throw();
        }
    }

    private string GetPartitionLeasePrefix()
    {
        return this.containerNamePrefix + ContainerSeparator + PartitionPrefix;
    }
    ync Task InitializeAsync()
    {
        Uri collectionUri = UriFactory.CreateDocumentCollectionUri(this.leaseStoreCollectionInfo.DatabaseName, this.leaseStoreCollectionInfo.CollectionName);
        DocumentCollection collection = await this.client.ReadDocumentCollectionAsync(collectionUri);
        this.leaseStoreCollectionLink = collection.SelfLink;

        // Get delta between server and local time for time synchonization.
        DateTime snapshot1 = DateTime.UtcNow;
        Document dummyDocument = await this.client.CreateDocumentAsync(this.leaseStoreCollectionLink, new Document { Id = this.GetDocumentId() + Guid.NewGuid().ToString() });
        DateTime snapshot2 = DateTime.UtcNow;
        this.serverToLocalTimeDelta = dummyDocument.Timestamp.ToUniversalTime() - new DateTime((snapshot1.Ticks + snapshot2.Ticks) / 2, DateTimeKind.Utc);
        await this.client.DeleteDocumentAsync(dummyDocument.SelfLink);

        TraceLog.Verbose(string.Format("Server to local time delta: {0}", this.serverToLocalTimeDelta));
    }

    public async Task<bool> LeaseStoreExistsAsync()
    {
        var containerDocument = await this.TryGetLease(this.GetDocumentId());
        return containerDocument != null ? true : false;
    }

    public async Task<bool> CreateLeaseStoreIfNotExistsAsync()
    {
        bool wasCreated = false;

        if (!await this.LeaseStoreExistsAsync())
        {
            var containerDocument = new Document { Id = this.GetDocumentId() };

            try
            {
                await this.client.CreateDocumentAsync(this.leaseStoreCollectionLink, containerDocument);
                wasCreated = true;
            }
            catch (DocumentClientException ex)
            {
                if (StatusCode.Conflict != (StatusCode)ex.StatusCode)
                {
                    throw;
                }
            }
        }

        return wasCreated;
    }

    public Task<IEnumerable<DocumentServiceLease>> ListLeases()
    {
        return this.ListDocuments(this.GetPartitionLeasePrefix());
    }

    /// <summary>
    /// Checks whether lease exists and creates if does not exist.
    /// </summary>
    /// <returns>true if created, false otherwise.</returns>
    public async Task<bool> CreateLeaseIfNotExistAsync(string partitionId, string continuationToken)
    {
        bool wasCreated = false;
        var leaseDocId = this.GetDocumentId(partitionId);
        if (await this.TryGetLease(leaseDocId) == null)
        {
            try
            {
                await this.client.CreateDocumentAsync(
                    this.leaseStoreCollectionLink,
                    new DocumentServiceLease { Id = leaseDocId, PartitionId = partitionId, ContinuationToken = continuationToken });
                wasCreated = true;
            }
            catch (DocumentClientException ex)
            {
                if (StatusCode.Conflict != (StatusCode)ex.StatusCode)
                {
                    throw;
                }
            }
        }

        return wasCreated;
    }

    public async Task<DocumentServiceLease> GetLeaseAsync(string partitionId)
    {
        return await this.TryGetLease(this.GetDocumentId(partitionId));
    }

    public async Task<DocumentServiceLease> AcquireAsync(DocumentServiceLease lease, string owner)
    {
        if (lease == null || lease.PartitionId == null)
        {
            throw new ArgumentException("lease");
        }

        if (string.IsNullOrWhiteSpace(owner))
        {
            throw new ArgumentException("owner");
        }

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
        });
    }

    public async Task<DocumentServiceLease> RenewAsync(DocumentServiceLease lease)
    {
        Debug.Assert(lease != null, "lease");

        DocumentServiceLease refreshedLease = await this.TryGetLease(this.GetDocumentId(lease.PartitionId));
        if (refreshedLease == null)
        {
            TraceLog.Informational(string.Format("Failed to renew lease for partition id {0}! The lease is gone already.", lease.PartitionId));
            throw new LeaseLostException(lease);
        }
        else if (refreshedLease.Owner != lease.Owner)
        {
            TraceLog.Informational(string.Format("Failed to renew lease for partition id {0}! The lease was already taken by another host.", lease.PartitionId));
            throw new LeaseLostException(lease);
        }

        return await this.UpdateInternalAsync(refreshedLease, serverLease => serverLease);
    }

    public async Task<bool> ReleaseAsync(DocumentServiceLease lease)
    {
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
    }


    public async Task DeleteAsync(DocumentServiceLease lease)
    {
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
        }
    }

    public async Task DeleteAllAsync()
    {
        var docs = await this.ListDocuments(this.containerNamePrefix);
        foreach (var doc in docs)
        {
            DocumentServiceLease lease = new DocumentServiceLease(doc);
            await this.DeleteAsync(lease);
        }
    }

    public Task<bool> IsExpired(DocumentServiceLease lease)
    {
        Debug.Assert(lease != null);

        return Task.FromResult<bool>(lease.Timestamp.ToUniversalTime() + this.leaseInterval + this.leaseIntervalAllowance < DateTime.UtcNow + this.serverToLocalTimeDelta);
    }

    public async Task<Lease> CheckpointAsync(Lease lease, string continuationToken, long sequenceNumber)
    {
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
    }

    private async Task<DocumentServiceLease> UpdateInternalAsync(
            DocumentServiceLease lease,
            LeaseConflictResolver conflictResolver,
            string owner = null)
    {
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

                    lease = conflictResolver(serverLease);
                }
                else
                {
                    throw new LeaseLostException(lease);
                }
            }
        }
    }

    private async Task<Document> TryGetDocument(string documentId)
    {
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
    }

    private async Task<DocumentServiceLease> TryGetLease(string documentId)
    {
        Document leaseDocument = await this.TryGetDocument(documentId);

        if (leaseDocument != null)
        {
            return new DocumentServiceLease(leaseDocument);
        }
        else
        {
            return null;
        }
    }

    private Task<IEnumerable<DocumentServiceLease>> ListDocuments(string prefix)
    {
        Debug.Assert(!string.IsNullOrEmpty(prefix), "prefix");

        var querySpec = new SqlQuerySpec(
                string.Format(CultureInfo.InvariantCulture, "SELECT * FROM c WHERE STARTSWITH(c.id, @PartitionLeasePrefix)"),
                new SqlParameterCollection(new SqlParameter[] { new SqlParameter { Name = "@PartitionLeasePrefix", Value = prefix } }));
        var query = this.client.CreateDocumentQuery<DocumentServiceLease>(this.leaseStoreCollectionLink, querySpec);

        return Task.FromResult<IEnumerable<DocumentServiceLease>>(query.AsEnumerable<DocumentServiceLease>());
    }

    /// <summary>
    /// Creates id either for container (if partitionId parameter is empty) or for lease otherwise.
    /// </summary>
    /// <param name="partitionId">The lease partition id.</param>
    /// <returns>Document id for container or lease.</returns>
    private string GetDocumentId(string partitionId = null)
    {
        return string.IsNullOrEmpty(partitionId) ?
                this.containerNamePrefix + DocumentServiceLeaseManager.ContainerSeparator + DocumentServiceLeaseManager.ContainerNameSuffix :
                this.GetPartitionLeasePrefix() + partitionId;
    }

    private RequestOptions CreateIfMatchOptions(DocumentServiceLease lease)
    {
        Debug.Assert(lease != null, "lease");

        AccessCondition ifMatchCondition = new AccessCondition { Type = AccessConditionType.IfMatch, Condition = lease.ETag };
        return new RequestOptions { AccessCondition = ifMatchCondition };
    }

    private void HandleLeaseOperationException(DocumentServiceLease lease, ExceptionDispatchInfo dispatchInfo)
    {
        Debug.Assert(lease != null, "lease");
        Debug.Assert(dispatchInfo != null, "dispatchInfo");

        DocumentClientException dcex = (DocumentClientException)dispatchInfo.SourceException;
        TraceLog.Warning(string.Format("Lease operation exception, status code: ", dcex.StatusCode));

        if (StatusCode.PreconditionFailed == (StatusCode)dcex.StatusCode ||
                StatusCode.Conflict == (StatusCode)dcex.StatusCode ||
                StatusCode.NotFound == (StatusCode)dcex.StatusCode)
        {
            throw new LeaseLostException(lease, dcex, StatusCode.NotFound == (StatusCode)dcex.StatusCode);
        }
        else
        {
            dispatchInfo.Throw();
        }
    }

    private string GetPartitionLeasePrefix()
    {
        return this.containerNamePrefix + ContainerSeparator + PartitionPrefix;
    }
}

}