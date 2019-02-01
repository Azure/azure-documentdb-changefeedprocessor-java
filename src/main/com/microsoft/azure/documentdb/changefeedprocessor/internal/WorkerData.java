/**
 * The MIT License (MIT)
 * Copyright (c) 2016 Microsoft Corporation
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
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
