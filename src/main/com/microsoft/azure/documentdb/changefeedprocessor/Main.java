/**
 * The MIT License (MIT)
 * Copyright (c) 2016 Microsoft Corporation
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

import com.microsoft.azure.documentdb.*;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.ChangeFeedObserverFactory;

import java.lang.*;
import java.net.URI;
import java.util.Scanner;

/*
 *
 * @author moderakh
 *
 */
public class Main {

    public static void main(String[] args) throws Exception{

        Scanner scanner = new Scanner(System.in);

        System.out.print("URL: ");
        String url = scanner.nextLine();

        System.out.print("MasterKey: ");
        String masterKey = scanner.nextLine();

        System.out.print("Database: ");
        String database = scanner.nextLine();

        System.out.print("Collection: ");
        String collection = scanner.nextLine();

        testChangeFeed("localhost", url, database, collection, masterKey);
    }

    public static void testChangeFeed(String hostname, String url, String database, String collection, String masterKey) throws Exception{
        System.out.println("Test: ChangeFeed");

        DocumentCollectionInfo docColInfo = new DocumentCollectionInfo();
        docColInfo.setUri(new URI(url));
        docColInfo.setDatabaseName(database);
        docColInfo.setCollectionName(collection);
        docColInfo.setMasterKey(masterKey);

        ChangeFeedOptions defaultFeedOptions = new ChangeFeedOptions();
        ChangeFeedHostOptions defaultHostOptions = new ChangeFeedHostOptions();

        DocumentCollectionInfo docColInfoaux = new DocumentCollectionInfo(docColInfo);

        ChangeFeedEventHost host = new ChangeFeedEventHost(hostname, docColInfo, docColInfoaux, defaultFeedOptions, defaultHostOptions);

        host.registerObserver(TestChangeFeedObserver.class);
    }

}
