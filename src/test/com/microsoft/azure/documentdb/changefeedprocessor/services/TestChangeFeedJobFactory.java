package com.microsoft.azure.documentdb.changefeedprocessor.services;

public class TestChangeFeedJobFactory implements JobFactory {

    @Override
    public Job create() {
        return new TestJob();
    }
}
