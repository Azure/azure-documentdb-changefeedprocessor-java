# Why should you use this code?

Azure Cosmos DB is a fast and flexible globally replicated database, well-suited for IoT, gaming, retail, and operational logging applications. A common design pattern in these applications is to use changes to the data to kick off additional actions. Chek the documentation for possibilities of usage [Cosmos DB Change Feed documentation](https://docs.microsoft.com/en-us/azure/cosmos-db/change-feed).

This Java library will allow you to read the changes that are made in your collection, before use this library or any othre you should take this points in consideration.

The change feed has the following properties:

    Changes are persistent in DocumentDB and can be processed asynchronously.

    Changes to documents within a collection are available immediately in the change feed.

    Each change to a document appears only once in the change feed. Only the most recent change for a given document is included in the change log. Intermediate changes may not be available.

    The change feed is sorted by order of modification within each partition key value. There is no guaranteed order across partition-key values.

    Changes can be synchronized from any point-in-time, that is, there is no fixed data retention period for which changes are available.

    Changes are available in chunks of partition key ranges. This capability allows changes from large collections to be processed in parallel by multiple consumers/servers.

    Applications can request for multiple Change Feeds simultaneously on the same collection.

    Delete operation its not considerated as change, if you want to handle delete you must add a delete flag property.

See more at [Azure Cosmos DB Change Feed support doc](https://azure.microsoft.com/en-us/blog/introducing-change-feed-support-in-azure-documentdb/)

## About

The Java code was built based on the C# library ChangeFeedProcessor [Link to the Repo](https://github.com/Azure/azure-documentdb-dotnet/tree/master/samples/ChangeFeedProcessor) after some customer request and was develop in a togheter effort with Azure Cosmos DB product team and CSE (Commercial Software Engineering) team.

## Understanding the code

The architecture is basically the same of the C# library, we  have splited the code into its diffent responsability to allow better testing and understanding.

The code is divided into four main parts:

1. ChangeFeedEventHost

    This is the entry point of the application and it is responsible to initiate all the objects and start the work, as soon as we register an Observer.

    The code can handle ONE Collection at a time, to handle more than one collection it is necessary to instantiate a new ChangeFeedEventHost Object.

1. DocumentServiceLeaseManager

    This class handles the leases for the partition to avoid different jobs to query the ChangeFeed.

1. PartitionManager

    Handles the partitions available into the collection, that will be processed. As soon as the lease is acquiried the envent onPartitionAcquired is called to start the job.

1. ChangeFeedJob

    It is the actual job that will be performed againt the partition in the colleciton, it basically query the data using the number of docs defined in ChangeFeedOptions. If the job is updated it waits for some time.

## How to use

The basic usage of this library is covered at [samples folder](./samples/Readme.md), there you will find a simple application and the explanation.

## Dependencies

* Maven   4.0

* com.microsoft.azure.azure-documentdb    1.13.0

* junit   4.12

* org.slf4j.slf4j-simple  1.7.25

* org.projectlombok.lombok    1.16.18

## Contributing

This project welcomes contributions and suggestions.  Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit [Contributor License Agreement web site](https://cla.microsoft.com).

When you submit a pull request, a CLA-bot will automatically determine whether you need to provide
a CLA and decorate the PR appropriately (e.g., label, comment). Simply follow the instructions
provided by the bot. You will only need to do this once across all repos using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.