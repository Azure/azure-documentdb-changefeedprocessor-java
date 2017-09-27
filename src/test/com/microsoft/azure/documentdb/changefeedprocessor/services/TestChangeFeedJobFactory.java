package com.microsoft.azure.documentdb.changefeedprocessor.services;

import com.microsoft.azure.documentdb.changefeedprocessor.DocumentCollectionInfo;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.ConfigurationException;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.ConfigurationFile;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Dictionary;
import java.util.HashSet;

public class TestChangeFeedJobFactory implements JobFactory {

    HashSet<String> activeJobs;

    public TestChangeFeedJobFactory() {
        activeJobs = new HashSet<>();
    }
    @Override
    public Job create() {
        return new TestJob(this);
    }

    public void enable(String partitionId) {
        activeJobs.add(partitionId);
    }

    public void disable(String partitionId) {
        activeJobs.remove(partitionId);
    }

    public int count() {
        return activeJobs.size();
    }

    public boolean isActive(String partitionId) {
        return activeJobs.contains(partitionId);
    }
}
