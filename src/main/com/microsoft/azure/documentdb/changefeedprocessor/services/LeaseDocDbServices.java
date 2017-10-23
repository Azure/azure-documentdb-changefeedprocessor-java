package com.microsoft.azure.documentdb.changefeedprocessor.services;

import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.changefeedprocessor.ChangeFeedHostOptions;
import com.microsoft.azure.documentdb.changefeedprocessor.ChangeFeedObserverCloseReason;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.*;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.documentleasestore.*;

public class LeaseDocDbServices extends LeaseServices implements IPartitionObserver<DocumentServiceLease> {
    private final PartitionManager<DocumentServiceLease> partitionManager;
    private final DocumentServiceLeaseManager leaseManager;
    private final ChangeFeedHostOptions options;

    public LeaseDocDbServices(DocumentServiceLeaseManager documentServiceLeaseManager, PartitionManager<DocumentServiceLease> partitionManager, ChangeFeedHostOptions options) {
        this.leaseManager = documentServiceLeaseManager;
        this.partitionManager = partitionManager;
        this.options = options;
    }

    @Override
    public void subscribe(ILeaseSubscriber subscriber) {
    }

    public void init() throws DocumentClientException, LeaseLostException {
        leaseManager.initialize();

//        this.checkpointSvcs = new Refactor.CheckpointServices((ICheckpointManager)leaseManager, this.options.CheckpointFrequency);

        if (this.options.getDiscardExistingLeases()) {
            //TraceLog.Warning(string.Format("Host '{0}': removing all leases, as requested by ChangeFeedHostOptions", this.HostName));
            this.leaseManager.deleteAll();
        }

        // Note: lease store is never stale as we use monitored colleciton Rid as id prefix for aux collection.
        // Collection was removed and re-created, the rid would change.
        // If it's not deleted, it's not stale. If it's deleted, it's not stale as it doesn't exist.
        this.leaseManager.createLeaseStoreIfNotExists();

        // this.createLeases

        // OLD SYNTAX
        this.partitionManager.SubscribeAsync(this);

        this.partitionManager.initialize();
    }

    @Override
    public void onPartitionAcquired(DocumentServiceLease lease) {
        String partitionId = lease.getPartitionId();

        acquire(partitionId);
    }

    @Override
    public void onPartitionReleased(DocumentServiceLease lease, ChangeFeedObserverCloseReason reason) {
        String partitionId = lease.getPartitionId();

        release(partitionId);
    }
}
