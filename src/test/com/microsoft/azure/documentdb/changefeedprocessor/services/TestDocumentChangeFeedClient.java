package com.microsoft.azure.documentdb.changefeedprocessor.services;

import com.microsoft.azure.documentdb.Document;

import java.util.ArrayList;
import java.util.List;

public class TestDocumentChangeFeedClient extends DocumentChangeFeedClient {

    private final String continuation;
    private final String partitionId;
    private int counter;

    public TestDocumentChangeFeedClient(String partitionId, String continuation) {
        this.partitionId = partitionId;
        this.continuation = continuation;
        counter = 0;
    }

    public List<Document> read() throws DocumentChangeFeedException {
        List<Document> list = new ArrayList<>();

        // add some sleep
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        list.add(new Document("{a: 1, t: " + counter++ +"}"));
        list.add(new Document("{b: 2, t: " + counter++ +"}"));
        list.add(new Document("{c: 3, t: " + counter++ +"}"));

        return list;
    }

}
