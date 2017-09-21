package com.microsoft.azure.documentdb.changefeedprocessor;

import com.microsoft.azure.documentdb.Document;

import java.util.List;
import java.util.concurrent.Future;

public class TestChangeFeedObserver implements IChangeFeedObserver {
    public void open(ChangeFeedObserverContext context) {
    }

    public void close(ChangeFeedObserverContext context, ChangeFeedObserverCloseReason reason) {
    }

    public void processChanges(ChangeFeedObserverContext context, List<Document> docs) {
        for(Document d : docs) {
            String content = d.toJson();

            System.out.println("Received: " + content);
        }
    }
}
