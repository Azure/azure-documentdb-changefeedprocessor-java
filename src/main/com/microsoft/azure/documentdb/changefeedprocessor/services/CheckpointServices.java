package com.microsoft.azure.documentdb.changefeedprocessor.services;

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
}
