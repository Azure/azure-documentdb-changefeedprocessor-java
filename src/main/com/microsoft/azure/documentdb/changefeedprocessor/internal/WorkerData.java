package com.microsoft.azure.documentdb.changefeedprocessor.internal;

import com.microsoft.azure.documentdb.changefeedprocessor.ChangeFeedObserverContext;
import com.microsoft.azure.documentdb.changefeedprocessor.IChangeFeedObserver;

import java.util.concurrent.Future;

public class WorkerData
{
    private Future task;
    private IChangeFeedObserver observer;
    private ChangeFeedObserverContext context;
    private CancellationTokenSource cancellation;

    public WorkerData(Future task, IChangeFeedObserver observer, ChangeFeedObserverContext context, CancellationTokenSource cancellation)
    {
        this.task = task;
        this.observer = observer;
        this.context = context;
        this.cancellation = cancellation;
    }


    public Future getTask() {
        return task;
    }

    public void setTask(Future _task) {
        this.task = _task;
    }

    public IChangeFeedObserver getObserver() {
        return observer;
    }

    public void setObserver(IChangeFeedObserver _observer) {
        this.observer = _observer;
    }

    public ChangeFeedObserverContext getContext() {
        return context;
    }

    public void setContext(ChangeFeedObserverContext _context) {
        this.context = _context;
    }

    public CancellationTokenSource getCancellation() {
        return cancellation;
    }

    public void setCancellation(CancellationTokenSource cancellation) {
        this.cancellation = cancellation;
    }
}
