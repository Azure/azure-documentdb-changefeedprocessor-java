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
import java.util.Scanner;

public class SimpleTest {

    @Test
    public void testCreateChangeFeedHostUsingSecrets()  {
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

        ChangeFeedEventHost host = new ChangeFeedEventHost("hotsname", docInfo, docAux, options, new ChangeFeedHostOptions() );
        Assert.assertNotNull(host);

        IChangeFeedObserverFactory factory = new ChangeFeedObserverFactory(TestChangeFeedObserver.class);
        
        try {
            host.registerObserverFactory(factory);

            System.out.println("Press ENTER to finish");
            Scanner scanner = new Scanner(System.in);
            scanner.nextLine();
        }
        catch(Exception e) {
            e.printStackTrace();
            Assert.fail("failed");
        }
    }
}
