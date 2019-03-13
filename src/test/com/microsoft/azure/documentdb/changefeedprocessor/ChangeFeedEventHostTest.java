/*
 * The MIT License (MIT)
 * Copyright (c) 2018 Microsoft Corporation
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package com.microsoft.azure.documentdb.changefeedprocessor;

import com.microsoft.azure.documentdb.ChangeFeedOptions;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.*;
import org.junit.Assert;
import org.junit.Test;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class ChangeFeedEventHostTest {

    Logger logger = Logger.getLogger(ChangeFeedEventHostTest.class.getName());

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

    @Test
    public void createChangeFeedHostUsingSecrets()  {
        ConfigurationFile config = null;

        try {
            config = new ConfigurationFile("app.secrets");
        } catch (ConfigurationException e) {
            Assert.fail(e.getMessage());
        }

        DocumentCollectionInfo docInfo = new DocumentCollectionInfo();
        try {
            docInfo.setUri(new URI(config.get("COSMOSDB_ENDPOINT")));
            docInfo.setMasterKey(config.get("COSMOSDB_SECRET"));
            docInfo.setDatabaseName(config.get("COSMOSDB_DATABASE"));
            docInfo.setCollectionName(config.get("COSMOSDB_COLLECTION"));
        } catch (URISyntaxException e) {
            Assert.fail("COSMOSDB URI FAIL: " + e.getMessage());
        } catch (ConfigurationException e) {
            Assert.fail("Configuration Error " + e.getMessage());

        }

        DocumentCollectionInfo docAux = new DocumentCollectionInfo(docInfo);

        try {
            docAux.setCollectionName(config.get("COSMOSDB_AUX_COLLECTION"));
        } catch (ConfigurationException e) {
            Assert.fail("Configuration Error " + e.getMessage());
        }

        ChangeFeedOptions options = new ChangeFeedOptions();
        options.setPageSize(100);

        ChangeFeedHostOptions hostOptions = new ChangeFeedHostOptions();
        hostOptions.setDiscardExistingLeases(true);

        ChangeFeedEventHost host = new ChangeFeedEventHost("hostname", docInfo, docAux, options, hostOptions );
        Assert.assertNotNull(host);

        IChangeFeedObserverFactory factory = new ChangeFeedObserverFactory(TestChangeFeedObserver.class);
        try {
            host.registerObserverFactory(factory);

            while(!host.getExecutorService().isTerminated() &&
                    !host.getExecutorService().isShutdown()){
                logger.info("Host Service is Running");
                host.getExecutorService().awaitTermination(5, TimeUnit.MINUTES);
            }
        }
        catch(Exception e) {
            Assert.fail("registerObserverfactory exception " + e.getMessage());
        }
    }
}
