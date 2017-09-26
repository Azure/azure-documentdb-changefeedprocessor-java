package com.microsoft.azure.documentdb.changefeedprocessor.services;

import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.changefeedprocessor.IChangeFeedObserverFactory;

import java.util.Enumeration;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

public class ResourcePartitionCollection implements Iterable<ResourcePartition> {
    private ConcurrentHashMap<String, ResourcePartition> resourcePartitions;

    public ResourcePartitionCollection() {
        resourcePartitions = new ConcurrentHashMap<>();
    }

    public void put(String partitionId, ResourcePartition resourcePartition) {
        resourcePartitions.put(partitionId, resourcePartition);
    }

    public ResourcePartition get(String partitionId) {
        return resourcePartitions.get(partitionId);
    }

    @Override
    public Iterator<ResourcePartition> iterator() {
        return resourcePartitions.values().iterator();
    }
}