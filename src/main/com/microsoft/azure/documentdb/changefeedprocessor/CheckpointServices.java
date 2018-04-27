//package com.microsoft.azure.documentdb.changefeedprocessor.services;
package com.microsoft.azure.documentdb.changefeedprocessor;

import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.changefeedprocessor.CheckpointFrequency;
import com.microsoft.azure.documentdb.changefeedprocessor.CheckpointStats;
import com.microsoft.azure.documentdb.changefeedprocessor.ICheckpointManager;
import com.microsoft.azure.documentdb.changefeedprocessor.ILeaseManager;
import com.microsoft.azure.documentdb.changefeedprocessor.DocumentServiceLease;

import java.time.Instant;
import java.util.logging.Logger;

public class CheckpointServices {

    private CheckpointFrequency checkpointOptions;
    private ICheckpointManager checkpointManager;
    private ILeaseManager<DocumentServiceLease> leaseManager;
    private Logger logger = Logger.getLogger(CheckpointServices.class.getName());

    public CheckpointServices(ILeaseManager<DocumentServiceLease> leaseManager, CheckpointFrequency checkpointOptions){
        this.leaseManager = leaseManager;
        this.checkpointManager = (ICheckpointManager)leaseManager;
        this.checkpointOptions = checkpointOptions;
    }

    public String getCheckpointData(String partitionId) throws DocumentClientException {
        String data = getCheckpoint(partitionId);
        if (data == null)
            data = "";
        logger.info(String.format("Retrieving Checkpoint partitionId: %s, data: %s",partitionId,data));

        return data;
    }

    public void setCheckpointData(String partitionId, String data) throws DocumentClientException {
        logger.info(String.format("Saving Checkpoint partitionId: %s, data: %s",partitionId,data));
        DocumentServiceLease lease = null;
        try {
        	// CR: perf/RU impact: if we kept the lease in the loop in checkpoint job, we didn't have to get it every time.
        	//     Probably OK for now, but we'll need to fix that.
            lease = (DocumentServiceLease)leaseManager.getLease(partitionId).call();
        } catch (Exception e) {
        	// CR: add a comment that checkpoint will throw if lease is null.
            e.printStackTrace();
        }
        String continuation = data;
        checkpoint(lease, continuation);
    }

    String getCheckpoint(String partitionId) throws DocumentClientException {
        DocumentServiceLease lease = null;
        try {
            lease = (DocumentServiceLease) this.leaseManager.getLease(partitionId).call();
        } catch (Exception e) {
            logger.severe(String.format("Error!! %s", e.getMessage()));
            e.printStackTrace();
        }
        return lease.getContinuationToken();
    }

    void checkpoint(DocumentServiceLease lease, String continuation) {
        assert lease == null;
        assert continuation == null || continuation == "";

        DocumentServiceLease result = null;
        try
        {
            result = (DocumentServiceLease) (this.checkpointManager.checkpoint(lease, continuation, lease.getSequenceNumber() + 1)).call();

            if(result.getConcurrencyToken() == continuation) {
                assert result.getConcurrencyToken() == continuation : "ContinuationToken was not updated" ;
                logger.info(String.format("Checkpoint: partition %s, continuation token '%s' was not updated!", lease.getPartitionId(), continuation));
            }
            else
                logger.info(String.format("Checkpoint: partition %s, new continuation '%s'", lease.getPartitionId(), continuation));
        }
        catch (Exception ex)
        {
            logger.severe(String.format("Partition %s: failed to checkpoint due to unexpected error: $s", lease.getPartitionId(), ex.getMessage()));
            
        }
    }
    
    // CR: this doesn't seem to be called from anywhere. Need to implement the feature.
    boolean isCheckpointNeeded(DocumentServiceLease lease, CheckpointStats checkpointStats)
    {
        CheckpointFrequency options = this.checkpointOptions;

        assert lease != null;
        assert checkpointStats != null;

        if (checkpointStats.getProcessedDocCount() == 0) {
            return false;
        }

        boolean isCheckpointNeeded = true;

        boolean hasProcessedDocumentCount = options.getProcessedDocumentCount().isPresent();
        boolean hasTimeInterval = options.getTimeInterval().isPresent();

        if (options != null && (hasProcessedDocumentCount || hasTimeInterval)) {
            // Note: if either condition is satisfied, we checkpoint.
            isCheckpointNeeded = false;

            if (hasProcessedDocumentCount) {
                isCheckpointNeeded = (checkpointStats.getProcessedDocCount() >= options.getProcessedDocumentCount().get());
            }

            if (hasTimeInterval) {
                isCheckpointNeeded = isCheckpointNeeded ||
                        (Instant.now().getEpochSecond() - checkpointStats.getLastCheckpointTime().getEpochSecond()) >= options.getTimeInterval().get();
            }
        }

        return isCheckpointNeeded;
    }
}