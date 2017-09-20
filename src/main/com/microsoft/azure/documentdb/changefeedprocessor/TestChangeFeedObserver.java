package com.microsoft.azure.documentdb.changefeedprocessor;

import com.microsoft.azure.documentdb.Document;

import java.util.List;
import java.util.concurrent.Future;

public class TestChangeFeedObserver implements IChangeFeedObserver {
    @Override
    public Future OpenTaskAsync(ChangeFeedObserverContext context) {
        return null;
    }

    @Override
    public Future CloseAsync(ChangeFeedObserverContext context, ChangeFeedObserverCloseReason reason) {
        return null;
    }

    @Override
    public Future ProcessChangesAsync(ChangeFeedObserverContext context, List<Document> docs) {
        return null;
    }
}
