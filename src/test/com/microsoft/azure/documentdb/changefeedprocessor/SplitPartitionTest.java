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

import static org.junit.Assert.fail;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.microsoft.azure.documentdb.ChangeFeedOptions;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.ResourceResponse;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.ChangeFeedThreadFactory;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.ConfigurationException;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.ConfigurationFile;
import com.microsoft.azure.documentdb.changefeedprocessor.services.DocumentServices;
import org.junit.Assert;
import org.junit.Test;

import java.net.URI;
import java.net.URISyntaxException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.Scanner;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Logger;

/**
 * @author Visouza
 *
 */
public class SplitPartitionTest
{
    private Logger logger = Logger.getLogger(SplitPartitionTest.class.getName());
    private final int CPUs = Runtime.getRuntime().availableProcessors();

    @Test
    public void test() {

        //Read properties from app.secrets
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

        DocumentServices documentServices = new DocumentServices(docInfo);

        //Write on Collection

        ExecutorService exec  = CreateExecutorService(1,"CosmosWriter");
        exec.execute(new Runnable() {
            @Override
            public void run() {
                try {

                    while(true && (!exec.isShutdown() || !exec.isTerminated())) {
                        ModelDocument document = new ModelDocument();
                        ResourceResponse response = documentServices.createDocument(document, true);
                        logger.info(String.format("Plant register recorded ID: %s", response.getResource().getId()));
                    }

                } catch (DocumentClientException e) {
                    logger.warning(e.getMessage());
                    e.printStackTrace();
                }
            }
        });


        //Runs ChangeFeedEventHost
        DocumentCollectionInfo docAux = new DocumentCollectionInfo(docInfo);

        try {
            docAux.setCollectionName(config.get("COSMOSDB_AUX_COLLECTION"));
        } catch (ConfigurationException e) {
            Assert.fail("Configuration Error " + e.getMessage());
        }

//        ChangeFeedOptions options = new ChangeFeedOptions();
//        options.setPageSize(100);
//
//        ChangeFeedEventHost host = new ChangeFeedEventHost("hotsname", docInfo, docAux, options, new ChangeFeedHostOptions() );
//        Assert.assertNotNull(host);

        try {
//            host.registerObserver(TestChangeFeedObserver.class);

            System.out.println("Press ENTER to finish");
            Scanner scanner = new Scanner(System.in);
            scanner.nextLine();
        }
        catch(Exception e) {
            e.printStackTrace();
            Assert.fail("failed");
        }


    }

     private ExecutorService CreateExecutorService(int numThreadPerCPU, String threadSuffixName ){

        logger.info(String.format("Creating ExecutorService CPUs: %d, numThreadPerCPU: %d, threadSuffixName: %s",CPUs, numThreadPerCPU, threadSuffixName));
        if (numThreadPerCPU <= 0) throw new IllegalArgumentException("The parameter numThreadPerCPU must be greater them 0");
        if (threadSuffixName == null || threadSuffixName.isEmpty()) throw new IllegalArgumentException("The parameter threadSuffixName is null or empty");

        ChangeFeedThreadFactory threadFactory = new ChangeFeedThreadFactory(threadSuffixName);
        ExecutorService exec = Executors.newFixedThreadPool(numThreadPerCPU * CPUs, threadFactory);

        //ExecutorService exec = Executors.newFixedThreadPool(numThreadPerCPU * CPUs, Executors.defaultThreadFactory());
        return exec;
    }


    private class ModelDocument{
        @JsonProperty("id")
        public String Id;
        public Double Temp;
        public Double Humidity;
        public String SensorDateTime;
        public String PlantType;
        public int Light;
        public int Room;
        public int PlantPosition;

        public ModelDocument(){
            Random rand = new Random();

            UUID uuid = UUID.randomUUID();
            Id = uuid.toString();
            Temp = rand.nextDouble();
            Humidity = rand.nextDouble();

            DateFormat df = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss a");
            SensorDateTime = df.format(new Date(System.currentTimeMillis()));
            PlantType = "Demo";
            Light = 300;
            Room = 0;
            PlantPosition = -1;
        }
    }

}
