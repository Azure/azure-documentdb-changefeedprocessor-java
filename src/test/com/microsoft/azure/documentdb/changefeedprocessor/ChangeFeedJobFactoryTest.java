package com.microsoft.azure.documentdb.changefeedprocessor;

import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.changefeedprocessor.services.*;
import org.junit.Assert;
import org.junit.Test;

public class ChangeFeedJobFactoryTest {

    @Test
    public void testJob() throws DocumentClientException, InterruptedException {
        DocumentServices documentServices = new TestDocumentServices();
        CheckpointServices checkpointServices = new CheckpointInMemoryServices();
        TestChangeFeedObserver observerFactory = new TestChangeFeedObserver();
        JobServices jobServices = new JobServices();

        ChangeFeedJob2 job = new ChangeFeedJob2(documentServices, checkpointServices, observerFactory);

        // documentServices and observerFactory
        jobServices.runAsync(job, "0");

        int initialDocumentCount = -1;

        // wait 1 second
        int totalSleepTime = 1000; // 1 second
        int maxInteractions = 100;

        // check if initialDocumentCount value was changed
        for(int i=0; i<maxInteractions ; i++) {
            initialDocumentCount = observerFactory.getDocumentCount();
            if( initialDocumentCount > 0 )
                break;
            Thread.sleep(1 + totalSleepTime/maxInteractions);
        }

        Assert.assertTrue("job is running", job.checkIsRunning());
        Assert.assertTrue("documents were processed", initialDocumentCount > 0);

        // stop job
        job.stop();

        // wait 1 second
        for(int i=0; i<maxInteractions ; i++) {
            if( job.checkIsRunning() == false )
                break;
            Thread.sleep(1 + totalSleepTime/maxInteractions);
        }

        Assert.assertFalse("job is stopped",  job.checkIsRunning() );
    }

    // test jobfactory - does it create one per partition
    // each worker receives a different partitionId
}
