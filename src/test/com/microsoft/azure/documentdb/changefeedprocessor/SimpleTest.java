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
    public void testCreatChangeFeedHostUsingSecrets() throws Exception {
        ConfigurationFile config = new ConfigurationFile("app.secrets");

        String url = config.get("COSMOSDB_ENDPOINT");
        String database = config.get("COSMOSDB_DATABASE");
        String collection = config.get("COSMOSDB_COLLECTION");
        String masterKey = config.get("COSMOSDB_SECRET");
        String auxCollection = config.get("COSMOSDB_AUX_COLLECTION");

        Main.testChangeFeed2("hostname", url, database, collection, masterKey);

        Thread.sleep(10000);
    }
}
