package com.microsoft.azure.documentdb.changefeedprocessor;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;

import com.microsoft.azure.documentdb.ChangeFeedOptions;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.ConfigurationException;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.ConfigurationFile;
import com.microsoft.azure.documentdb.changefeedprocessor.services.DocumentCollectionInfo;

public class HostTest {
	public static void main(String[] args) {
		
	
        ConfigurationFile config = null;

//        try {
//            config = new ConfigurationFile("app.secrets");
//        } catch (ConfigurationException e) {
//            Assert.fail(e.getMessage());
//        }

        DocumentCollectionInfo docInfo = new DocumentCollectionInfo();
        try {
            docInfo.setUri(new URI("https://greenhousedemo.documents.azure.com:443"));
            docInfo.setMasterKey("oM3OfljlXr44geMYkjNf4oeSlH3AQsLfktcioiQfgcREr07eJNf1q9eO0RjNmEZAdDDQOQNBTt8NwWyBCvZs9Q==");
            docInfo.setDatabaseName("greenhouseiotdemo");
            docInfo.setCollectionName("changefeedtest");
        } catch (URISyntaxException e) {
            Assert.fail("COSMOSDB URI FAIL: " + e.getMessage());
        } 

        DocumentCollectionInfo docAux = new DocumentCollectionInfo(docInfo);

//        try {
//            docAux.setCollectionName(config.get("COSMOSDB_AUX_COLLECTION"));
//        } catch (ConfigurationException e) {
//            //Assert.fail("Configuration Error " + e.getMessage());
//        }

        ChangeFeedOptions options = new ChangeFeedOptions();
        options.setPageSize(100);

        ChangeFeedHostOptions hostOptions = new ChangeFeedHostOptions();
        hostOptions.setDiscardExistingLeases(true);



        try {
    		
        	final ChangeFeedEventHost	host = new ChangeFeedEventHost("hostname", docInfo, docAux, options, hostOptions );
    		
            Assert.assertNotNull(host);
            
            host.registerObserver(TestChangeFeedObserver.class);

//            while(!host.getExecutorService().isTerminated() &&
//                    !host.getExecutorService().isShutdown()){
//                logger.info("Host Service is Running");
//                host.getExecutorService().awaitTermination(5, TimeUnit.MINUTES);
//            }
            
            Executors.newCachedThreadPool().execute(()-> {
            	try {
					TimeUnit.MINUTES.sleep(1);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
            	host.unregisterObservers();
            });
        }
        catch(Exception e) {
            Assert.fail("registerObserver exception " + e.getMessage());
        }
    }
	
}
