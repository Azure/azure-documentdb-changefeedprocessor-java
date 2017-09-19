/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.microsoft.azure.documentdb.changefeedprocessor;


import com.microsoft.azure.documentdb.ChangeFeedOptions;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.IPartitionObserver;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.documentleasestore.DocumentServiceLease;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;

public class ChangeFeedEventHost implements IPartitionObserver<DocumentServiceLease> {


    private DocumentCollectionInfo _collectionLocation;
    private ChangeFeedOptions _changeFeedOptions;
    private ChangeFeedHostOptions _options;
    private String _hostName;
    DocumentCollectionInfo _auxCollectionLocation;
    ConcurrentMap<String, WorkerData> partitionKeyRangeIdToWorkerMap;



    public ChangeFeedEventHost( String hostName, DocumentCollectionInfo documentCollectionLocation, DocumentCollectionInfo auxCollectionLocation){
//        this(hostName, documentCollectionLocation, auxCollectionLocation, new ChangeFeedOptions(), new ChangeFeedHostOptions());
    }

    public ChangeFeedEventHost(
            String hostName,
            DocumentCollectionInfo documentCollectionLocation,
            DocumentCollectionInfo auxCollectionLocation,
            ChangeFeedOptions changeFeedOptions,
            ChangeFeedHostOptions hostOptions) {

        if (documentCollectionLocation == null) throw new IllegalArgumentException("documentCollectionLocation");
        if (documentCollectionLocation.getUri() == null) throw new IllegalArgumentException("documentCollectionLocation.getUri()");
        if (documentCollectionLocation.getDatabaseName() == null || documentCollectionLocation.getDatabaseName().isEmpty()) throw new IllegalArgumentException("documentCollectionLocation.getDatabaseName() is null or empty");
        if (documentCollectionLocation.getCollectionName() == null || documentCollectionLocation.getCollectionName().isEmpty()) throw new IllegalArgumentException("documentCollectionLocation.getCollectionName() is null or empty");
        if (hostOptions.getMinPartitionCount() > hostOptions.getMaxPartitionCount()) throw new IllegalArgumentException("hostOptions.MinPartitionCount cannot be greater than hostOptions.MaxPartitionCount");

        this.collectionLocation = CanoninicalizeCollectionInfo(documentCollectionLocation);
        this.changeFeedOptions = changeFeedOptions;
        this.options = hostOptions;
        this.HostName = hostName;
        this.auxCollectionLocation = CanoninicalizeCollectionInfo(auxCollectionLocation);
        this.partitionKeyRangeIdToWorkerMap = new ConcurrentDictionary<string, WorkerData>();


    }

    private class WorkerData
    {

        private Future _task;

        public WorkerData(Future task, IChangeFeedObserver observer, ChangeFeedObserverContext context, CancellationTokenSource cancellation)
        {
            this.Task = task;
            this.Observer = observer;
            this.Context = context;
            this.Cancellation = cancellation;
        }


        public Future Task { get; private set; }

        public IChangeFeedObserver Observer { get; private set; }

        public ChangeFeedObserverContext Context { get; private set; }

        public CancellationTokenSource Cancellation { get; private set; }
    }


}
