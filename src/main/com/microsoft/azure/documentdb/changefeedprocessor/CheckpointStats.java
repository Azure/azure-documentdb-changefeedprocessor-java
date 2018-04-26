package com.microsoft.azure.documentdb.changefeedprocessor;

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
}
