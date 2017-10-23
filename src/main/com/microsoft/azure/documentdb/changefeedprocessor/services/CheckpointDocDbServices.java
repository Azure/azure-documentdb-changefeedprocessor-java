package com.microsoft.azure.documentdb.changefeedprocessor.services;

import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.changefeedprocessor.CheckpointFrequency;
import com.microsoft.azure.documentdb.changefeedprocessor.CheckpointStats;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.ICheckpointManager;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.ILeaseManager;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.documentleasestore.DocumentServiceLease;

import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;

public class CheckpointDocDbServices implements CheckpointServices {

    //TODO: We are using this dictonary to save the continuations tokens for test only.
    ConcurrentHashMap<String, String> checkpoints;

    private CheckpointFrequency checkpointOptions;
    private ICheckpointManager checkpointManager;
    private ILeaseManager<DocumentServiceLease> leaseManager;

    public CheckpointDocDbServices(){
        this.checkpoints = new ConcurrentHashMap<>();
    }

    public String getContinuationToken(String partitionId) throws DocumentClientException {
        //return getCheckpoint(partitionId);
        return this.checkpoints.get(partitionId);
    }

    public void setContinuationToken(String partitionId, String data) throws DocumentClientException {
        //DocumentServiceLease lease = (DocumentServiceLease)leaseManager.getLease(partitionId);
        String continuation = (String)data;
        //checkpoint(lease, continuation);
        this.checkpoints.put(partitionId, continuation);
    }

//    String getCheckpoint(String partitionId) throws DocumentClientException {
//        DocumentServiceLease lease = (DocumentServiceLease) this.leaseManager.getLease(partitionId);
//
//        return lease.getContinuationToken();
//    }
//    public void checkpoint(DocumentServiceLease lease, String continuation) {
//        assert (lease != null);
//        assert (continuation != null && continuation != "");
//
//        DocumentServiceLease result = null;
//        try
//        {
//            result = (DocumentServiceLease) this.checkpointManager.checkpoint(lease, continuation, lease.getSequenceNumber() + 1);
//
//            assert (result.getConcurrencyToken() == continuation ); // "ContinuationToken was not updated!"
////            TraceLog.Informational(string.Format("Checkpoint: partition {0}, new continuation '{1}'", lease.PartitionId, continuation));
//        }
////        catch (LeaseLostException ex)
////        {
//////            TraceLog.Warning(string.Format("Partition {0}: failed to checkpoint due to lost lease", context.PartitionKeyRangeId));
////            throw ex;
////        }
//        catch (Exception ex)
//        {
////            TraceLog.Error(string.Format("Partition {0}: failed to checkpoint due to unexpected error: {1}", context.PartitionKeyRangeId, ex.Message));
//            throw ex;
//        }
//    }
}