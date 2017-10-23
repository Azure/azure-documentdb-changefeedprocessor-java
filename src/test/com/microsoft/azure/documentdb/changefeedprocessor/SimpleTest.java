package com.microsoft.azure.documentdb.changefeedprocessor;

import com.microsoft.azure.documentdb.ChangeFeedOptions;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.ConfigurationException;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.ConfigurationFile;
import org.junit.Assert;
import org.junit.Test;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Scanner;

public class SimpleTest {

    @Test
    public void testCreateChangeFeedHostUsingSecrets()  {
        ConfigurationFile config = null;

        String url = config.get("COSMOSDB_ENDPOINT");
        String database = config.get("COSMOSDB_DATABASE");
        String collection = config.get("COSMOSDB_COLLECTION");
        String masterKey = config.get("COSMOSDB_SECRET");
        String auxCollection = config.get("COSMOSDB_AUX_COLLECTION");

        Main.testChangeFeed2("hostname", url, database, collection, masterKey);
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

        try {
            host.registerObserver(TestChangeFeedObserver.class);

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
