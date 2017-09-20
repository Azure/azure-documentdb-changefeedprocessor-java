package com.microsoft.azure.documentdb.changefeedprocessor.services;

import java.util.Dictionary;
import java.util.Hashtable;

public class ResourcePartitionServices {
    Dictionary<String, ResourcePartition> _resourcePartitions;
    DocumentServicesClient _client;

    public ResourcePartitionServices(DocumentServicesClient client) {

        _resourcePartitions = new Hashtable<>();
        _client = client;
    }

    public ResourcePartition create(String partitionId) {
        ResourcePartition resourcePartition = new ResourcePartition(partitionId, new ChangeFeedJob(_client));

        _resourcePartitions.put(partitionId, resourcePartition);

        return resourcePartition;
    }

    public ResourcePartition get(String partitionId) {
        return _resourcePartitions.get(partitionId);
    }
}