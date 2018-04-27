package com.microsoft.azure.documentdb.changefeedprocessor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.microsoft.azure.documentdb.ChangeFeedOptions;
import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.ConfigurationException;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.ConfigurationFile;
import com.microsoft.azure.documentdb.changefeedprocessor.services.DocumentCollectionInfo;

public class SimpleE2ETest {
	


	static List<Document> docs;
	static ChangeFeedEventHost host;
	
	public static void createDoc() {
		

				ConfigurationFile config = null;
		        DocumentCollectionInfo docInfo = new DocumentCollectionInfo();
		        DocumentCollectionInfo auxDocInfo = new DocumentCollectionInfo();
		        DocumentClient client = null;
		        try {
		        	config = new ConfigurationFile("app.secrets");
			        String endpoint = config.get("COSMOSDB_ENDPOINT");
					String secret = config.get("COSMOSDB_SECRET");
					String databaseName = config.get("COSMOSDB_DATABASE");
					String collectionName = config.get("COSMOSDB_COLLECTION");
					String auxCollectionName = config.get("COSMOSDB_AUX_COLLECTION");
					
					docInfo.setUri(new URI(endpoint));
		            docInfo.setMasterKey(secret);
		            docInfo.setDatabaseName(databaseName);
		            docInfo.setCollectionName(collectionName);
		            
		            auxDocInfo.setUri(new URI(endpoint));
		        	auxDocInfo.setMasterKey(secret);
		        	auxDocInfo.setDatabaseName(databaseName);
					auxDocInfo.setCollectionName(auxCollectionName);
					
					Document documentDefinition1 = new Document(
			                "{" +
			                        "   \"id\": \""+Instant.now().toEpochMilli()+"\"," +
			                        "   \"part\" : \"Seattle\"," +
			                        "   \"ts\" : \""+Instant.now().toString()+"\"" +
		
			                "} ") ;
					
					Document documentDefinition2 = new Document(
			                "{" +
			                		"   \"id\": \""+Instant.now().toEpochMilli()+"\"," +
			                        "   \"part\" : \"Seattle\"," +
			                        "   \"ts\" : \""+Instant.now().toString()+"\"" +
		
			                "} ") ;
					
					String collectionLink = String.format("/dbs/%s/colls/%s", databaseName, collectionName);
					
					client = new DocumentClient(endpoint, secret, null, null);
					
					client.createDocument(collectionLink, documentDefinition1, null, false);
					client.createDocument(collectionLink, documentDefinition2, null, false);
					
					
		            
		        } catch (URISyntaxException | ConfigurationException | DocumentClientException e) {
		        	e.printStackTrace();
		            Assert.fail("COSMOSDB URI FAIL: " + e.getMessage());
		        } finally {
		        	if(client!=null) {
		        		client.close();
		        	}
		        }
		
		        DocumentCollectionInfo docAux = new DocumentCollectionInfo(auxDocInfo);
		
		
		        ChangeFeedOptions options = new ChangeFeedOptions();
		        options.setPageSize(100);
		
		        ChangeFeedHostOptions hostOptions = new ChangeFeedHostOptions();
		        hostOptions.setDiscardExistingLeases(true);
		
				try {
					host = new ChangeFeedEventHost("hostname", docInfo, docAux, options, hostOptions );
					Assert.assertNotNull(host);
					host.registerObserver(TestChangeFeedObserver.class);
				} catch (Exception e2) {
					e2.printStackTrace();
				}
		
		

			
				try {
					TimeUnit.SECONDS.sleep(30);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				try {
					host.unregisterObservers();
				} catch (Exception e) {
					e.printStackTrace();
				}

			
		
    }
	
	public static void main(String[] args) {
		
	
        
		createDoc();
		System.out.println(SimpleE2ETest.docs.size());
		
		
    }
	
	@Test
	public void check() {
		createDoc();
		try {
			Thread.sleep(30);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		assertEquals(2, SimpleE2ETest.docs.size());
	}
	
	
	public static class TestChangeFeedObserver implements IChangeFeedObserver {

		@Override
		public Callable<Void> open(ChangeFeedObserverContext context) {
			return null;
		}

		@Override
		public Callable<Void> close(ChangeFeedObserverContext context, ChangeFeedObserverCloseReason reason) {
			return null;
		}

		@Override
		
		public Callable<Void> processChanges(ChangeFeedObserverContext context, List<Document> docs) {
			for (Document d : docs) {
                String content = d.toJson();
                System.out.println("Received: " + content);
            }
			SimpleE2ETest.docs = docs;
	        return null;
		}

	}
	
	

}
