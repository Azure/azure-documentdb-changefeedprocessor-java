package com.microsoft.azure.documentdb.changefeedprocessor.services;

import com.microsoft.azure.documentdb.DocumentClientException;

public class TestJob implements Job {
    @Override
    public void start(Object initialData) throws DocumentClientException, InterruptedException {
        System.out.println("started");
    }

    @Override
    public void stop() {
        System.out.println("stopped");
    }
}
