package com.microsoft.azure.documentdb.changefeedprocessor.services;

import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.changefeedprocessor.CheckpointFrequency;
import com.microsoft.azure.documentdb.changefeedprocessor.CheckpointStats;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.ICheckpointManager;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.ILeaseManager;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.documentleasestore.DocumentServiceLease;

import java.time.Instant;
import java.util.logging.Logger;

public class CheckpointServices {

    private CheckpointFrequency checkpointOptions;
    private ICheckpointManager checkpointManager;
    private ILeaseManager leaseManager;
    private Logger logger = Logger.getLogger(CheckpointServices.class.getName());

    public CheckpointServices(ILeaseManager leaseManager, CheckpointFrequency checkpointOptions){
        this.leaseManager = leaseManager;
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
        DocumentServiceLease lease = (DocumentServiceLease)leaseManager.getLease(partitionId);
        String continuation = data;
        checkpoint(lease, continuation);
    }

    String getCheckpoint(String partitionId) throws DocumentClientException {
        DocumentServiceLease lease = (DocumentServiceLease) this.leaseManager.getLease(partitionId);
        return lease.getContinuationToken();
    }

    public void checkpoint(DocumentServiceLease lease, String continuation) {
        assert (lease != null);
        assert (continuation != null && continuation != "");

        DocumentServiceLease result = null;
        try
        {
            result = (DocumentServiceLease) this.checkpointManager.checkpoint(lease, continuation, lease.getSequenceNumber() + 1);

            assert (result.getConcurrencyToken() == continuation ); // "ContinuationToken was not updated!"
            logger.info(String.format("Checkpoint: partition %s, new continuation '%s'", lease.getPartitionId(), continuation));
        }
        catch (Exception ex)
        {
            logger.severe(String.format("Partition %s: failed to checkpoint due to unexpected error: $s", lease.getPartitionId(), ex.getMessage()));
            throw ex;
        }
    }
    public boolean isCheckpointNeeded(DocumentServiceLease lease, CheckpointStats checkpointStats)
    {
        CheckpointFrequency options = this.checkpointOptions;

        assert (lease != null);
        assert (checkpointStats != null);

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