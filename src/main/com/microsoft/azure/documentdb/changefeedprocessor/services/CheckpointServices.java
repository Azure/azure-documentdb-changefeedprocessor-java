package com.microsoft.azure.documentdb.changefeedprocessor.services;

import java.util.Dictionary;
import java.util.Hashtable;

public class CheckpointServices {

    //TODO: We are using this dictonary to save the continuations tokens for test only.
    Dictionary<String, String> _checkpoints;

    public CheckpointServices(){
        _checkpoints = new Hashtable<String, String>();
    }

    public Object getCheckpointData(String partitionId) {
        return _checkpoints.get(partitionId);
    }

    public void setCheckpointData(String partitionId, Object data) {
        _checkpoints.put(partitionId, (String)data);
    }
}
