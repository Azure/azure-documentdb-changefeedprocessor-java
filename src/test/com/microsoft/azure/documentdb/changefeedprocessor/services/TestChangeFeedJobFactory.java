package com.microsoft.azure.documentdb.changefeedprocessor.services;

import com.microsoft.azure.documentdb.changefeedprocessor.DocumentCollectionInfo;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.ConfigurationException;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.ConfigurationFile;

import java.net.URI;
import java.net.URISyntaxException;

public class TestChangeFeedJobFactory implements JobFactory {

    @Override
    public Job create() {
        return new TestJob();
    }
}
