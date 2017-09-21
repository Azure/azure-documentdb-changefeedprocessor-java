package com.microsoft.azure.documentdb.changefeedprocessor.services;

import com.microsoft.azure.documentdb.changefeedprocessor.CheckpointFrequency;
import com.microsoft.azure.documentdb.changefeedprocessor.CheckpointStats;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.documentleasestore.DocumentServiceLease;

import java.time.Instant;
import java.util.Dictionary;
import java.util.Hashtable;

public class CheckpointServices {

    //TODO: We are using this dictonary to save the continuations tokens for test only.
    Dictionary<String, String> checkpoints;

    private CheckpointFrequency _checkpointOptions;

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
