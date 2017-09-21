package com.microsoft.azure.documentdb.changefeedprocessor.services;

import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.changefeedprocessor.CheckpointFrequency;
import com.microsoft.azure.documentdb.changefeedprocessor.CheckpointStats;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.ICheckpointManager;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.ILeaseManager;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.LeaseLostException;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.documentleasestore.DocumentServiceLease;

import java.time.Instant;
import java.util.Dictionary;
import java.util.Hashtable;

public class CheckpointServices {

    //TODO: We are using this dictonary to save the continuations tokens for test only.
    Dictionary<String, String> checkpoints;

    private CheckpointFrequency _checkpointOptions;
    private ICheckpointManager _checkpointManager;
    private ILeaseManager<DocumentServiceLease> _leaseManager;

    public CheckpointServices(){
        checkpoints = new Hashtable<String, String>();
    }

    public Object getCheckpointData(String partitionId) throws DocumentClientException {
        return getCheckpoint(partitionId);
        //return checkpoints.get(partitionId);
    }

    public void setCheckpointData(String partitionId, Object data) throws DocumentClientException {
        DocumentServiceLease lease = (DocumentServiceLease)_leaseManager.getLease(partitionId);

        String continuation = lease.getContinuationToken();

        checkpoint(lease, continuation);
        //checkpoints.put(partitionId, (String)data);
    }

    String getCheckpoint(String partitionId) throws DocumentClientException {
        DocumentServiceLease lease = (DocumentServiceLease)_leaseManager.getLease(partitionId);

        return lease.getContinuationToken();
    }
    public void checkpoint(DocumentServiceLease lease, String continuation) {
        assert (lease != null);
        assert (continuation != null && continuation != "");

        DocumentServiceLease result = null;
        try
        {
            result = (DocumentServiceLease) _checkpointManager.checkpoint(lease, continuation, lease.getSequenceNumber() + 1);

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
        CheckpointFrequency options = _checkpointOptions;

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