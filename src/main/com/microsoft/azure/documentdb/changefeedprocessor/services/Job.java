package com.microsoft.azure.documentdb.changefeedprocessor.services;

import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.changefeedprocessor.ChangeFeedObserverCloseReason;

public interface Job {
    void start(String initialData) throws DocumentClientException, InterruptedException;
    void stop(ChangeFeedObserverCloseReason CloseReason);
}
