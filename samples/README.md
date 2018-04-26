# Samples

We stronglly suggest the usage of a properties file to load all the configuration required.

We provided a basic template at the root of this repo [app.secrets.template](./app.secrets.template)

``` Properties
COSMOSDB_ENDPOINT=https://<cosmos name>.documents.azure.com:443
COSMOSDB_SECRET=<cosmosdb secret>
COSMOSDB_DATABASE=<cosmos database name>
COSMOSDB_COLLECTION=<cosmos collection name>
COSMOSDB_AUX_COLLECTION=<cosmos auxiliar collection name>
COSMOSDB_LEASE_COLLECTION=<cosmos collection name for leases>
```

## 1. Loading the properties file

To make it easier to handle the properties file we made a ConfigurationFile available at internal Package.

First instantiate a ConfigurationFile Object passing the propertie file name as a parameter.

``` JAVA
ConfigurationFile config = null;

try {
    config = new ConfigurationFile("app.secrets");
} catch (ConfigurationException e) {
    Assert.fail(e.getMessage());
}
```

Then use the method get to read the value of an property

``` JAVA
config.get("COSMOSDB_ENDPOINT");
```

## 2. Document Collection Info

To store all the data required to connect to Cosmos DB and the collection that will be used to read the ChangeFeed, we use a class DocumentCollectionInfo.

``` JAVA
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
```

The same class will be used to store the Collection Lease information.

``` JAVA
DocumentCollectionInfo docAux = new DocumentCollectionInfo(docInfo);

try {
    docAux.setCollectionName(config.get("COSMOSDB_AUX_COLLECTION"));
} catch (ConfigurationException e) {
    Assert.fail("Configuration Error " + e.getMessage());
}

```

## 3. Setting the Change Feed options

There are two classes that is responsible to set how the ChangeFeedEventHost will work, the ChangeFeedOptions and ChangeFeedHostOptions.

Using ChangeFeedOptions we can set the number of docs that will be read for each request made by ChangeFeedJob, for instance.

``` JAVA
ChangeFeedOptions options = new ChangeFeedOptions();
options.setPageSize(100);
```

ChangeFeedHostOptions can be used for high level settings like deleting all the lease information, available to the lease collection.

Obs: By doing that all the work in progress will be lost.

``` JAVA
ChangeFeedHostOptions hostOptions = new ChangeFeedHostOptions();
hostOptions.setDiscardExistingLeases(true);
```

## 4. Instantiating ChangeFeedEventHost

To start consuming the ChangeFeed we need to instantiate a ChangeFeedEventHost object, by providing all the hostname, the collections and the options.

``` JAVA
ChangeFeedEventHost host = new ChangeFeedEventHost("hostname", docInfo, docAux, options, hostOptions );
```

## 5. The Observer

The code realy on the observer to actually perform some action to the docs that are read from change feed.

We have made available a test observer that just print the docs received at the console. the class is on Test Package TestChangeFeedObserver.

To register an Observer use the method RegisterObserver.

``` JAVA
try {
    host.registerObserver(TestChangeFeedObserver.class);
}
catch(Exception e) {
    Assert.fail("registerObserver exception " + e.getMessage());
}
```

By registering an Observer we create an ExecutorService behind the scenes, this way you can control the execution flow.

The final code will be like this.

```JAVA
 try {
    host.registerObserver(TestChangeFeedObserver.class);

    while(!host.getExecutorService().isTerminated() &&
            !host.getExecutorService().isShutdown()){
        logger.info("Host Service is Running");
        host.getExecutorService().awaitTermination(5, TimeUnit.MINUTES);
    }
}
catch(Exception e) {
    Assert.fail("registerObserver exception " + e.getMessage());
}
```

To access a real class of this working please access the following link [Sample](../src/test/com/microsoft/azure/documentdb/changefeedprocessor/Sample.java)

## 6. Running the sample

To run the sample you will need an IDE (InteliJ, Eclipse, Netbeans and etc.) choose one that has the best fit with you. The IDE´s already has the Maven available and this make things easy to test.

If you want to test it in the CommandLine first you will need to generate the jar, by using Maven. To install it click [here](https://maven.apache.org/install.html).

```CMD
mvn package dependency:copy-dependencies package -DskipTests
```

With the jar available on ./target folder run the following command.

```CMD
java -cp ".\target\test-classes;.\target\classes;.\target\dependency\*" com.microsoft.azure.documentdb.changefeedprocessor.Sample
```
