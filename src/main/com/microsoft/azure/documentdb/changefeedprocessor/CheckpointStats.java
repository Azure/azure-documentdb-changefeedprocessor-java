package com.microsoft.azure.documentdb.changefeedprocessor;

import java.time.Instant;

public class CheckpointStats
{
    private int _processedDocCount;
    private Instant _lastCheckpointTime;

    public void reset() {
        this._processedDocCount = 0;
        this._lastCheckpointTime = Instant.now();
    }

    public int getProcessedDocCount() {
        return _processedDocCount;
    }

    public Instant getLastCheckpointTime() {
        return _lastCheckpointTime;
    }

    public void setProcessedDocCount(int processedDocCount) {
        this._processedDocCount = processedDocCount;
    }

    public void setLastCheckpointTime(Instant lastCheckpointTime) {
        _lastCheckpointTime = lastCheckpointTime;
    }
}
