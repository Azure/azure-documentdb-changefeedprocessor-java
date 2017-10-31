package com.microsoft.azure.documentdb.changefeedprocessor;

import com.microsoft.azure.documentdb.ChangeFeedOptions;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.ConfigurationException;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.ConfigurationFile;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.TimeUnit;

public class Sample {


    public static void main (String[]args) {

        ConfigurationFile config = null;

        try {
            config = new ConfigurationFile("app.secrets");
        } catch (ConfigurationException e) {
            System.out.println(e.getMessage());
        }

        DocumentCollectionInfo docInfo = new DocumentCollectionInfo();
        try {
            docInfo.setUri(new URI(config.get("COSMOSDB_ENDPOINT")));
            docInfo.setMasterKey(config.get("COSMOSDB_SECRET"));
            docInfo.setDatabaseName(config.get("COSMOSDB_DATABASE"));
            docInfo.setCollectionName(config.get("COSMOSDB_COLLECTION"));
        } catch (URISyntaxException e) {
            System.out.println("COSMOSDB URI FAIL: " + e.getMessage());
        } catch (ConfigurationException e) {
            System.out.println("Configuration Error " + e.getMessage());

        }

        DocumentCollectionInfo docAux = new DocumentCollectionInfo(docInfo);

        try {
            docAux.setCollectionName(config.get("COSMOSDB_AUX_COLLECTION"));
        } catch (ConfigurationException e) {
            System.out.println("Configuration Error " + e.getMessage());
        }

        ChangeFeedOptions options = new ChangeFeedOptions();
        options.setPageSize(100);

        ChangeFeedHostOptions hostOptions = new ChangeFeedHostOptions();
        hostOptions.setDiscardExistingLeases(false);

        ChangeFeedEventHost host = new ChangeFeedEventHost("hostname", docInfo, docAux, options, hostOptions );

        try {
            host.registerObserver(TestChangeFeedObserver.class);

            while(!host.getExecutorService().isTerminated() &&
                    !host.getExecutorService().isShutdown()){
                System.out.println("Host Service is Running");
                host.getExecutorService().awaitTermination(5, TimeUnit.MINUTES);
            }
        }
        catch(Exception e) {
            System.out.println("registerObserver exception " + e.getMessage());
        }

    }

}
