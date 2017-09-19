package com.microsoft.azure.documentdb.changefeedprocessor.internal.documentLeaseStore;

/**
 * Created by yoterada on 2017/09/19.
 */
public enum LeaseState {
    /**
     * UNSPECIFIED: The lease is in unknown state.
     */
    UNSPECIFIED,
    /**
     * AVAILABLE: The lease is available in the sense that it is not own, or leased, by any host.
     */
    AVAILABLE,
    /**
     * LEASED: The lease is leased to, or owned by some host.
     */
    LEASED;
}
