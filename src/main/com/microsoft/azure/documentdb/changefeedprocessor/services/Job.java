package com.microsoft.azure.documentdb.changefeedprocessor.services;

public interface Job {
    void start(Object initialData);
    void stop();
}
