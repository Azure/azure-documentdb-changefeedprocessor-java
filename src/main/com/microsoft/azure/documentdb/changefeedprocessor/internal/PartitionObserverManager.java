package com.microsoft.azure.documentdb.changefeedprocessor.internal;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.ArrayList;

import com.microsoft.azure.documentdb.changefeedprocessor.ChangeFeedObserverCloseReason;


final class PartitionObserverManager<T extends Lease> {
    final PartitionManager<T> partitionManager;
    final List<IPartitionObserver<T>> observers;

    public PartitionObserverManager(PartitionManager<T> partitionManager){
        this.partitionManager = partitionManager;
        this.observers = new ArrayList<IPartitionObserver<T>>();
    }

    // TODO: implement subscribeAsync ,notifyPartitionAcquiredAsync,  notifyPartitionReleasedAsync

    public IDisposable subscribe(IPartitionObserver<T> observer){
    if (!this.observers.contains(observer)){
        this.observers.add(observer);

        for (T lease : this.partitionManager.currentlyOwnedPartitions.values()){
            try{
              //  await observer.OnPartitionAcquiredAsync(lease);
            	observer.onPartitionAcquired(lease);
            }
            catch (Exception ex){
                // Eat any exceptions during notification of observers
                TraceLog.exception(ex);
            }
        }
    }

    return new Unsubscriber(this.observers, observer);
    }

    public void notifyPartitionAcquired(T lease){
        for (IPartitionObserver<T> obs : this.observers){
            obs.onPartitionAcquired(lease);
            //await obs.onPartitionAcquiredAsync(lease);
        }
    }

    public void notifyPartitionReleased(T lease, ChangeFeedObserverCloseReason reason){
        for (IPartitionObserver<T> obs : this.observers){
            obs.onPartitionReleased(lease, reason);
            //await obs.onPartitionReleasedAsync(lease, reason);
        }
    }
}

