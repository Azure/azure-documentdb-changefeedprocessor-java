package com.microsoft.azure.documentdb.changefeedprocessor.internal;

import com.microsoft.azure.documentdb.changefeedprocessor.ChangeFeedObserverCloseReason;


public class PartitionObserverManager<T extends Lease> {
//    readonly PartitionManager<T> partitionManager;
//    readonly List<IPartitionObserver<T>> observers;

    public PartitionObserverManager(PartitionManager<T> partitionManager)
    {
//        this.partitionManager = partitionManager;
//        this.observers = new List<IPartitionObserver<T>>();
    }

    // TODO: implements subscribe

    public IObservableDisposable subscribeAsync(IPartitionObserver<T> observer)
    {
//    if (!this.observers.Contains(observer))
//    {
//        this.observers.Add(observer);
//
//        foreach (var lease in this.partitionManager.currentlyOwnedPartitions.Values)
//        {
//            try
//            {
//                await observer.OnPartitionAcquiredAsync(lease);
//            }
//            catch (Exception ex)
//            {
//                // Eat any exceptions during notification of observers
//                TraceLog.Exception(ex);
//            }
//        }
//    }
//
//    return new Unsubscriber(this.observers, observer);
        return null;
    }

    public void notifyPartitionAcquired(T lease)
    {
//        foreach (var observer in this.observers)
//        {
//            await observer.OnPartitionAcquiredAsync(lease);
//        }
    }

    public void notifyPartitionReleased(T lease, ChangeFeedObserverCloseReason reason)
    {
//        foreach (var observer in this.observers)
//        {
//            await observer.OnPartitionReleasedAsync(lease, reason);
//        }
    }
}

