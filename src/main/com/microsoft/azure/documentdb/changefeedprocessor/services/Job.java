package com.microsoft.azure.documentdb.changefeedprocessor.services;

import com.microsoft.azure.documentdb.DocumentClientException;

public interface Job {
    void start(Object initialData) throws DocumentClientException, InterruptedException;
    void stop();
}
