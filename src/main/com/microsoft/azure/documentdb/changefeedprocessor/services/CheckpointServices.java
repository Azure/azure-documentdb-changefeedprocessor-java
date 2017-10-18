package com.microsoft.azure.documentdb.changefeedprocessor.services;

import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.changefeedprocessor.CheckpointFrequency;
import com.microsoft.azure.documentdb.changefeedprocessor.CheckpointStats;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.ICheckpointManager;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.ILeaseManager;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.documentleasestore.DocumentServiceLease;

import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

public class CheckpointServices {

    //TODO: We are using this dictionary to save the continuations tokens for test only.
    ConcurrentHashMap<String, String> checkpoints;

    private CheckpointFrequency checkpointOptions;
    private ICheckpointManager checkpointManager;
    private ILeaseManager<DocumentServiceLease> leaseManager;
    private Logger logger = Logger.getLogger(CheckpointServices.class.getName());

    public CheckpointServices(){
        this.checkpoints = new ConcurrentHashMap<>();
    }

    public String getCheckpointData(String partitionId) throws DocumentClientException {
        //String data = getCheckpoint(partitionId);
        String data = this.checkpoints.get(partitionId);
        if (data == null)
            data = "";
        logger.info(String.format("Retrieving Checkpoint partitionId: %s, data: %s",partitionId,data));

        return data;
    }

    public void setCheckpointData(String partitionId, String data) throws DocumentClientException {
        logger.info(String.format("Saving Checkpoint partitionId: %s, data: %s",partitionId,data));
        //DocumentServiceLease lease = (DocumentServiceLease)leaseManager.getLease(partitionId);
        String continuation = data;
        //checkpoint(lease, continuation);
        this.checkpoints.put(partitionId, continuation);
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
//            TraceLog.Informational(string.Format("Checkpoint: partition {0}, new continuation '{1}'", lease.PartitionId, continuation));
        }
//        catch (LeaseLostException ex)
//        {
////            TraceLog.Warning(string.Format("Partition {0}: failed to checkpoint due to lost lease", context.PartitionKeyRangeId));
//            throw ex;
//        }
        catch (Exception ex)
        {
//            TraceLog.Error(string.Format("Partition {0}: failed to checkpoint due to unexpected error: {1}", context.PartitionKeyRangeId, ex.Message));
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