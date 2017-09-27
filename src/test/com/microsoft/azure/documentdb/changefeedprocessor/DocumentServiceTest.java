package com.microsoft.azure.documentdb.changefeedprocessor;

import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.CFTestConfiguration;
import com.microsoft.azure.documentdb.changefeedprocessor.services.DocumentChangeFeedClient;
import com.microsoft.azure.documentdb.changefeedprocessor.services.DocumentChangeFeedException;
import com.microsoft.azure.documentdb.changefeedprocessor.services.DocumentServices;
import com.microsoft.azure.documentdb.changefeedprocessor.services.TestChangeFeedJobFactory;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class DocumentServiceTest {

    @Test
    public void testConnectionToCosmosChangeFeed() throws DocumentChangeFeedException {

        // retrieve test configuration
        DocumentCollectionInfo docInfo = CFTestConfiguration.getDefaultDocumentCollectionInfo();
        Assert.assertNotNull(docInfo);

        // list the partitions
        DocumentServices documentServices = new DocumentServices(docInfo);
        List<String> partitionNames = documentServices.listPartitionRange();
        Assert.assertTrue("partition names" , partitionNames.size() >  0);

        // create a partition client
        String partitionId = partitionNames.get(0); // use partition "0"
        DocumentChangeFeedClient client = documentServices.createClient(partitionId, null, 2);
        Assert.assertNotNull(client);

        // retrieve data
        List<Document> docs = client.read();
        String continuationToken = client.getContinuationToken();

        List<Document> docs2 = client.read();
        String continuationToken2 = client.getContinuationToken();

        // create another client using continuation token
        DocumentChangeFeedClient anotherClient = documentServices.createClient(partitionId, continuationToken, 2);
        Assert.assertNotNull(anotherClient);

        List<Document> anotherDocs = anotherClient.read();
        String anotherContinuationToken = anotherClient.getContinuationToken();

        // since it is a continuation, the document is different from the original client
        Assert.assertNotEquals(docs.get(0), anotherDocs.get(0));

        // but it is exactly the same as the second retrieval
        Assert.assertArrayEquals(docs2.toArray(), anotherDocs.toArray());
        Assert.assertEquals(continuationToken2, anotherContinuationToken);
    }

}
