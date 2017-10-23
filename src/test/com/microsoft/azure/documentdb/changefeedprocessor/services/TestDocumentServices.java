package com.microsoft.azure.documentdb.changefeedprocessor.services;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.ArrayList;
import java.util.List;

public class TestDocumentServices extends DocumentServices {
    List<String> partitionList;
    int partitionCount = 0;

    public TestDocumentServices() {
        partitionList = new ArrayList<>();

        newPartition();
        newPartition();
        newPartition();
    }

    @Override
    public List<String> listPartitionRange() {
        return partitionList;
    }

    public void newPartition() {
        partitionList.add("t" + partitionCount++);
    }

    @Override
    public DocumentChangeFeedClient createClient(String partitionId, String continuationToken) {
        return new TestDocumentChangeFeedClient(partitionId, continuationToken);
    }

    @Override
    public DocumentChangeFeedClient createClient(String partitionId, String continuationToken, int pageSize) {
        throw new NotImplementedException();
    }

}
