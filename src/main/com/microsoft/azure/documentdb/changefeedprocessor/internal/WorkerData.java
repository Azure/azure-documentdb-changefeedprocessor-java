package com.microsoft.azure.documentdb.changefeedprocessor.internal;

import com.microsoft.azure.documentdb.changefeedprocessor.ChangeFeedObserverContext;
import com.microsoft.azure.documentdb.changefeedprocessor.IChangeFeedObserver;

import java.util.concurrent.Future;

//Note (rogirdh): Removing CancellationTokenSource here. Not sure we need it since the task has a cancel method.

class WorkerData
{
    private Future<Void> task;
    private IChangeFeedObserver observer;
    private ChangeFeedObserverContext context;
    //private CancellationTokenSource cancellation;

    public WorkerData(Future<Void> task, IChangeFeedObserver observer, ChangeFeedObserverContext context/*, CancellationTokenSource cancellation*/)
    {
        this.task = task;
        this.observer = observer;
        this.context = context;
      //  this.cancellation = cancellation;
    }


    public Future<Void> getTask() {
        return task;
    }

    private void setTask(Future<Void> _task) {
        this.task = _task;
    }

    public IChangeFeedObserver getObserver() {
        return observer;
    }

    private void setObserver(IChangeFeedObserver _observer) {
        this.observer = _observer;
    }

    public ChangeFeedObserverContext getContext() {
        return context;
    }

    private void setContext(ChangeFeedObserverContext _context) {
        this.context = _context;
    }

    /*
    public CancellationTokenSource getCancellation() {
        return cancellation;
        return task.
    }

    private void setCancellation(CancellationTokenSource cancellation) {
        this.cancellation = cancellation;
    }
    */
}
