package com.microsoft.azure.documentdb.changefeedprocessor.internal;

import com.microsoft.azure.documentdb.changefeedprocessor.ChangeFeedObserverContext;
import com.microsoft.azure.documentdb.changefeedprocessor.IChangeFeedObserver;

import java.util.concurrent.Future;

public class WorkerData
{
    private Future _task;
    private IChangeFeedObserver _observer;
    private ChangeFeedObserverContext _context;
    private CancellationTokenSource _cancellation;

    public WorkerData(Future task, IChangeFeedObserver observer, ChangeFeedObserverContext context, CancellationTokenSource cancellation)
    {
        this._task = task;
        this._observer = observer;
        this._context = context;
        this._cancellation = cancellation;
    }


    public Future getTask() {
        return _task;
    }

    public void setTask(Future _task) {
        this._task = _task;
    }

    public IChangeFeedObserver getObserver() {
        return _observer;
    }

    public void setObserver(IChangeFeedObserver _observer) {
        this._observer = _observer;
    }

    public ChangeFeedObserverContext getContext() {
        return _context;
    }

    public void setContext(ChangeFeedObserverContext _context) {
        this._context = _context;
    }

    public CancellationTokenSource getCancellation() {
        return _cancellation;
    }

    public void set_cancellation(CancellationTokenSource _cancellation) {
        this._cancellation = _cancellation;
    }
}
