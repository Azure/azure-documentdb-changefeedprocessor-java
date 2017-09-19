package com.microsoft.azure.documentdb.changefeedprocessor;

import com.microsoft.azure.documentdb.ChangeFeedOptions;
import org.junit.Test;

public class ChangeFeedEventHostTest {

    @Test (expected = IllegalArgumentException.class)
    public void testInvalidConstructorHostname() throws IllegalArgumentException {
        ChangeFeedEventHost host = new ChangeFeedEventHost(
                null,
                new DocumentCollectionInfo(), new DocumentCollectionInfo(), new ChangeFeedOptions(), new ChangeFeedHostOptions());
    }
    @Test (expected = IllegalArgumentException.class)
    public void testInvalidConstructorDocumentCollectionInfo() throws IllegalArgumentException {
        ChangeFeedEventHost host = new ChangeFeedEventHost(
                "test",
                null, new DocumentCollectionInfo(), new ChangeFeedOptions(), new ChangeFeedHostOptions());
    }
    @Test (expected = IllegalArgumentException.class)
    public void testInvalidConstructorAuxCollectionInfo() throws IllegalArgumentException {
        ChangeFeedEventHost host = new ChangeFeedEventHost(
                "test",
                new DocumentCollectionInfo(), null, new ChangeFeedOptions(), new ChangeFeedHostOptions());
    }
    @Test (expected = IllegalArgumentException.class)
    public void testInvalidConstructorChangefeedOptions() throws IllegalArgumentException {
        ChangeFeedEventHost host = new ChangeFeedEventHost(
                "test",
                new DocumentCollectionInfo(), new DocumentCollectionInfo(), null, new ChangeFeedHostOptions());
    }
    @Test (expected = IllegalArgumentException.class)
    public void testInvalidConstructorChangefeedHostOptions() throws IllegalArgumentException {
        ChangeFeedEventHost host = new ChangeFeedEventHost(
                "test",
                new DocumentCollectionInfo(), new DocumentCollectionInfo(), new ChangeFeedOptions(), null);
    }
}
