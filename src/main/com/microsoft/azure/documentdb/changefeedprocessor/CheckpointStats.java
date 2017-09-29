package com.microsoft.azure.documentdb.changefeedprocessor;

import com.microsoft.azure.documentdb.changefeedprocessor.internal.documentleasestore.DocumentServiceLease;

import java.time.Instant;

public class CheckpointStats
{
    private int processedDocCount;
    private Instant lastCheckpointTime;

    public void reset() {
        this.processedDocCount = 0;
        this.lastCheckpointTime = Instant.now();
    }

    public int getProcessedDocCount() {
        return processedDocCount;
    }

    public Instant getLastCheckpointTime() {
        return lastCheckpointTime;
    }

    public void setProcessedDocCount(int processedDocCount) {
        this.processedDocCount = processedDocCount;
    }

    public void setLastCheckpointTime(Instant lastCheckpointTime) {
        this.lastCheckpointTime = lastCheckpointTime;
    }

    public boolean isCheckpointNeeded(CheckpointFrequency options)
    {
        CheckpointStats checkpointStats = this;

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
