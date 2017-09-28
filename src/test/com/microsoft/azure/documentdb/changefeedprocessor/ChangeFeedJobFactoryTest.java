package com.microsoft.azure.documentdb.changefeedprocessor;

import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.changefeedprocessor.services.*;
import org.junit.Test;

public class ChangeFeedJobFactoryTest {

    @Test
    public void testJob() throws DocumentClientException, InterruptedException {
        DocumentServices documentServices = new TestDocumentServices();
        CheckpointServices checkpointServices = new CheckpointInMemoryServices();
        TestChangeFeedObserver observerFactory = new TestChangeFeedObserver();

        ChangeFeedJob2 job = new ChangeFeedJob2(documentServices, checkpointServices, observerFactory);

        // documentServices and observerFactory
        job.start("0");

        // jobServices.runAsync
        // documentServices.push()
        // observerFactory.count()
        // sleep
        // check count

        // job.stop = stops?
    }

    // test jobfactory - does it create one per partition
    // each worker receives a different partitionId
}
