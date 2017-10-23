package com.microsoft.azure.documentdb.changefeedprocessor.services;

import java.util.List;

public class PartitionDocumentServices implements PartitionServices {

    private final DocumentServices documentServices;

    public PartitionDocumentServices(DocumentServices documentServices) {
        this.documentServices = documentServices;
    }

    @Override
    public ResourcePartitionCollection listPartitions() {

        ResourcePartitionCollection partitions = new ResourcePartitionCollection();
        List<String> partitionNames = documentServices.listPartitionRange();

        partitionNames
            .stream()
            .forEach( p -> partitions.put(p, new ResourcePartition(p)) );

        return partitions;
    }
}
