package com.microsoft.azure.documentdb.changefeedprocessor.services;

public class TestLeaseServices extends LeaseServices {

    @Override
    public void register(ResourcePartition partition) {
        // register first
        super.register(partition);
        // immediately acquire
        acquire(partition.getId());
    }

    @Override
    public void unregister(ResourcePartition partition) {
        // immediately release
        release(partition.getId());
        // then unregister
        super.unregister(partition);
    }

    @Override
    public void acquire(String partitionId) {
        super.acquire(partitionId);
    }

    @Override
    public void release(String partitionId) {
        super.release(partitionId);
    }
}
