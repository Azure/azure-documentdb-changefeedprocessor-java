package com.microsoft.azure.documentdb.changefeedprocessor.services;

import com.microsoft.azure.documentdb.changefeedprocessor.CheckpointStats;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.documentleasestore.DocumentServiceLease;

import java.util.Dictionary;
import java.util.Hashtable;

public class CheckpointServices {

    //TODO: We are using this dictonary to save the continuations tokens for test only.
    Dictionary<String, String> checkpoints;

    public CheckpointServices(){
        checkpoints = new Hashtable<String, String>();
    }

    public Object getCheckpointData(String partitionId) {
        return checkpoints.get(partitionId);
    }

    public void setCheckpointData(String partitionId, Object data) {
        checkpoints.put(partitionId, (String)data);
    }

    public boolean IsCheckpointNeeded(DocumentServiceLease lease, CheckpointStats checkpointStats)
    {
//        var options = new ChangeFeedHostOptions() { CheckpointFrequency = _checkpointOptions };
//
//        Debug.Assert(lease != null);
//        Debug.Assert(checkpointStats != null);
//
//        if (checkpointStats.ProcessedDocCount == 0)
//        {
//            return false;
//        }

        boolean isCheckpointNeeded = true;

//        if (options.CheckpointFrequency != null &&
//                (options.CheckpointFrequency.ProcessedDocumentCount.HasValue || options.CheckpointFrequency.TimeInterval.HasValue))
//        {
//            // Note: if either condition is satisfied, we checkpoint.
//            isCheckpointNeeded = false;
//            if (options.CheckpointFrequency.ProcessedDocumentCount.HasValue)
//            {
//                isCheckpointNeeded = checkpointStats.ProcessedDocCount >= options.CheckpointFrequency.ProcessedDocumentCount.Value;
//            }
//
//            if (options.CheckpointFrequency.TimeInterval.HasValue)
//            {
//                isCheckpointNeeded = isCheckpointNeeded ||
//                        DateTime.Now - checkpointStats.LastCheckpointTime >= options.CheckpointFrequency.TimeInterval.Value;
//            }
//        }

        return isCheckpointNeeded;
    }
}
