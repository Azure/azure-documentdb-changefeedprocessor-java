//package com.microsoft.azure.documentdb.changefeedprocessor.internal;
package com.microsoft.azure.documentdb.changefeedprocessor;

import java.util.List;

final class Unsubscriber<T extends Lease> implements AutoCloseable {

    final List<PartitionObserverInterface<T>> observers;
    final PartitionObserverInterface<T> observer;

    Unsubscriber(List<PartitionObserverInterface<T>> observers, PartitionObserverInterface<T> observer) {
        this.observers = observers;
        this.observer = observer;
    }

    public void close()
    {
        if (this.observers.contains(this.observer))
        {
            this.observers.remove(this.observer);
        }
    }
}
