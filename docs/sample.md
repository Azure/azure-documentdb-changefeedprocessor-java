
Sample
========

```java
    // choose target collection
    DocumentCollectionInfo docColInfo = new DocumentCollectionInfo();
    docColInfo.setUri(new URI(url));
    docColInfo.setDatabaseName(database);
    docColInfo.setCollectionName(collection);
    docColInfo.setMasterKey(masterKey);

    // configure lease document store
    DocumentCollectionInfo docColInfoaux = new DocumentCollectionInfo(docColAux);
    docColAux.setUri(new URI(urlAux));
    docColAux.setDatabaseName(databaseAux);
    docColAux.setCollectionName(collectionAux);
    docColAux.setMasterKey(masterKeyAux);

    // setup the Change Feed Event Host (version 2)
    ChangeFeedEventHost2 host = new ChangeFeedEventHost2(hostname, docColInfo, docColInfoaux);

    // register the observer and run
    host.registerObserver(TestChangeFeedObserver.class);
```


## Current Limitations ##

The current event host handles multi-partitioned collections, and delivers the messages to the 
assigned observer class. 

However, it is limited to a simple scenario
- single host
- no checkpoint
- no lease manager
- no split involved
