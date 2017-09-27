package com.microsoft.azure.documentdb.changefeedprocessor.services;

public class TestAutomaticLeaseServices extends LeaseServices {

    private final boolean isAutomatic;

    public TestAutomaticLeaseServices(boolean isAutomatic) {
        this.isAutomatic = isAutomatic;
    }

    @Override
    public void register(ResourcePartition partition) {
        // register first
        super.register(partition);

        if(isAutomatic) {
            // immediately acquire
            acquire(partition.getId());
        }
    }

    @Override
    public void unregister(ResourcePartition partition) {
        if(isAutomatic) {
        // immediately release
            release(partition.getId());
        }

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
