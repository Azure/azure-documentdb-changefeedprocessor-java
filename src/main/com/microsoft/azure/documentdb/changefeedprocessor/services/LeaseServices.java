package com.microsoft.azure.documentdb.changefeedprocessor.services;

public abstract class LeaseServices {
    private ResourcePartitionCollection partitions;
    private ILeaseSubscriber subscriber;

    public LeaseServices() {
        this.partitions = new ResourcePartitionCollection();
        this.subscriber = null;
    }

    public void register(ResourcePartition partition) {
        partitions.put(partition);
    }

    public void unregister(ResourcePartition partition) {
        partitions.remove(partition);
    }

    public void subscribe(ILeaseSubscriber subscriber) {
        this.subscriber = subscriber;
    }

    public void acquire(String partitionId) {
        if( subscriber != null ) {
            ResourcePartition partition = partitions.get(partitionId);

            if(partition != null) {
                subscriber.onLeaseAcquired(partition);
            }
        }
    }

    public void release(String partitionId) {
        if( subscriber != null ) {
            ResourcePartition partition = partitions.get(partitionId);

            if(partition != null) {
                subscriber.onLeaseReleased(partition);
            }
        }
    }
}
