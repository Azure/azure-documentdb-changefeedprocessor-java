package com.microsoft.azure.documentdb.changefeedprocessor.services;

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

}
