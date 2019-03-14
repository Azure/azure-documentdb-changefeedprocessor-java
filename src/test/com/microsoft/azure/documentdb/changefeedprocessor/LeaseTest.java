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

import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.ConfigurationException;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.ConfigurationFile;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.documentleasestore.DocumentServiceLeaseManager;
import com.microsoft.azure.documentdb.changefeedprocessor.services.DocumentServices;
import org.junit.Assert;
import org.junit.Test;

import java.net.URI;
import java.net.URISyntaxException;

public class LeaseTest {

    @Test
    public void LeaseInitialization() {
        //fail("Not yet implemented");

        ConfigurationFile config = null;

        try {
            config = new ConfigurationFile("app.secrets");
        } catch (ConfigurationException e) {
            Assert.fail(e.getMessage());
        }

        DocumentCollectionInfo docAux = new DocumentCollectionInfo();
        try {
            docAux.setUri(new URI(config.get("COSMOSDB_ENDPOINT")));
            docAux.setMasterKey(config.get("COSMOSDB_SECRET"));
            docAux.setDatabaseName(config.get("COSMOSDB_DATABASE"));
            docAux.setCollectionName(config.get("COSMOSDB_LEASE_COLLECTION"));
        } catch (URISyntaxException e) {
            Assert.fail("COSMOSDB URI FAIL: " + e.getMessage());
        } catch (ConfigurationException e) {
            Assert.fail("Configuration Error " + e.getMessage());
        }

        ChangeFeedHostOptions options = new ChangeFeedHostOptions();
        DocumentServices documentServices = new DocumentServices(docAux);

        DocumentServiceLeaseManager leaseManager = new DocumentServiceLeaseManager(
                "currentChangeFeedProcessorHost",
                docAux,
                "lease",
                options.getLeaseExpirationInterval(),
                options.getLeaseRenewInterval(),
                documentServices);

        try {
            leaseManager.initialize(true);
        } catch (DocumentClientException e) {
            e.printStackTrace();

            Assert.fail(e.getMessage());
        }

    }






}
