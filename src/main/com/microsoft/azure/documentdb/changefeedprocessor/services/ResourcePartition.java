package com.microsoft.azure.documentdb.changefeedprocessor.services;

public class ResourcePartition {
    String partitionId;
    String checkpointData;
    Job job;
}
