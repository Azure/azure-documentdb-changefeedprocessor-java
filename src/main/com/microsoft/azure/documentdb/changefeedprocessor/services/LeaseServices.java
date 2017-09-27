package com.microsoft.azure.documentdb.changefeedprocessor.services;

public abstract class LeaseServices {
    private ResourcePartitionCollection availablePartitions;
    private ResourcePartitionCollection leasedPartitions;
    private ILeaseSubscriber subscriber;

    public LeaseServices() {
        this.availablePartitions = new ResourcePartitionCollection();
        this.leasedPartitions = new ResourcePartitionCollection();
        this.subscriber = null;
    }

    public void register(ResourcePartition partition) {
        availablePartitions.put(partition);

        // todo: verify partition does not exist in Lease collection
    }

    public void unregister(ResourcePartition partition) {
        availablePartitions.remove(partition);

        // todo: verify partition is in available state
    }

    public void subscribe(ILeaseSubscriber subscriber) {
        this.subscriber = subscriber;
    }

    protected void acquire(String partitionId) {
        ResourcePartition partition = availablePartitions.get(partitionId);

        if(partition != null) {
            notifyAcquire(partition);
            changePartitionToLease(partition);
        }
    }

    protected void release(String partitionId) {
        ResourcePartition partition = leasedPartitions.get(partitionId);

        if(partition != null) {
            changePartitionToAvailable(partition);
            notifyRelease(partition);
        }
    }

    private void notifyAcquire(ResourcePartition partition) {
        if( (subscriber != null) && (partition != null)) {
            subscriber.onLeaseAcquired(partition);
        }
    }

    private void notifyRelease(ResourcePartition partition) {
        if( (subscriber != null) && (partition != null)) {
            subscriber.onLeaseReleased(partition);
        }
    }

    private void changePartitionToLease(ResourcePartition partition) {
        synchronized (leasedPartitions) {
            availablePartitions.remove(partition);
            leasedPartitions.put(partition);
        }
    }

    private void changePartitionToAvailable(ResourcePartition partition) {
        synchronized (leasedPartitions) {
            leasedPartitions.remove(partition);
            availablePartitions.put(partition);
        }
    }

}
