/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.microsoft.azure.documentdb.changefeedprocessor.services;

import com.microsoft.azure.documentdb.changefeedprocessor.ChangeFeedHostOptions;
import com.microsoft.azure.documentdb.changefeedprocessor.ChangeFeedObserverCloseReason;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.*;

/**
 *
 * @author yoterada
 */
public class SimplePartitionManager<T extends Lease> extends PartitionManager<T> {

    public SimplePartitionManager(String workerName, ILeaseManager<T> leaseManager, ChangeFeedHostOptions options)
    {
        super(workerName, leaseManager, options);
    }

    @Override
    public void initialize()
    {
    }

    @Override
    public void start()
    {
    }

    @Override
    public void stop(ChangeFeedObserverCloseReason reason)
    {
    }

    @Override
    public IDisposable SubscribeAsync(IPartitionObserver<T> observer)
    {
        return null;
    }

    @Override
    public void tryReleasePartition(String partitionId, boolean hasOwnership, ChangeFeedObserverCloseReason closeReason)
    {
    }

}
