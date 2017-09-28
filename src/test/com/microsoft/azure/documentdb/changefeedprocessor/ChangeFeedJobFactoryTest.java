package com.microsoft.azure.documentdb.changefeedprocessor;

import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.changefeedprocessor.services.ChangeFeedJob2;
import com.microsoft.azure.documentdb.changefeedprocessor.services.CheckpointServices;
import com.microsoft.azure.documentdb.changefeedprocessor.services.DocumentServices;
import com.microsoft.azure.documentdb.changefeedprocessor.services.TestDocumentServices;
import org.junit.Test;

public class ChangeFeedJobFactoryTest {

    @Test
    public void testJob() {
        DocumentServices documentServices = new TestDocumentServices();
//        CheckpointServices checkpointServices = new TestCheckpointServices();
//
//        ChangeFeedJob2 job = new ChangeFeedJob2(documentServices)
    }
}
