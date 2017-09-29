package com.microsoft.azure.documentdb.changefeedprocessor.services;

import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.changefeedprocessor.CheckpointFrequency;
import com.microsoft.azure.documentdb.changefeedprocessor.CheckpointStats;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.ILeaseManager;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.documentleasestore.DocumentServiceLease;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;

public class CheckpointInMemoryServices implements CheckpointServices {

    //TODO: We are using this dictonary to save the continuations tokens for test only.
    ConcurrentHashMap<String, String> checkpointStore;

    private CheckpointFrequency checkpointOptions;
    private ILeaseManager<DocumentServiceLease> leaseManager;

    public CheckpointInMemoryServices(){
        this.checkpointStore = new ConcurrentHashMap<>();
    }

    public String getContinuationToken(String partitionId) throws DocumentClientException {
        return checkpointStore.get(partitionId);
    }

    public void setContinuationToken(String partitionId, String continuation) throws DocumentClientException {
        checkpointStore.put(partitionId, continuation);
    }
}