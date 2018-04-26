package com.microsoft.azure.documentdb.changefeedprocessor;

import com.microsoft.azure.documentdb.DocumentClientException;

public interface Job {
    void start(String initialData, DocumentServiceLease dsl) throws DocumentClientException, InterruptedException;
    void stop(ChangeFeedObserverCloseReason CloseReason);
}
