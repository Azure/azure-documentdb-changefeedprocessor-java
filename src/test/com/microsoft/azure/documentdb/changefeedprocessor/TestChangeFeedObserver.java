/*
 * The MIT License (MIT)
 * Copyright (c) 2018 Microsoft Corporation
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
package com.microsoft.azure.documentdb.changefeedprocessor;

import com.microsoft.azure.documentdb.Document;

import java.util.List;
import java.util.concurrent.*;

public class TestChangeFeedObserver implements IChangeFeedObserver {

    private ExecutorService exec = null;

    public  TestChangeFeedObserver(){
        exec = Executors.newFixedThreadPool(1);
    }

    public void open(ChangeFeedObserverContext context) {
    }

    public void close(ChangeFeedObserverContext context, ChangeFeedObserverCloseReason reason) {

        switch (reason){
            case OBSERVER_ERROR:
            case UNKNOWN:
            case SHUTDOWN:
                exec.shutdown();
                break;
            case RESOURCE_GONE:
                exec.shutdownNow();
            case LEASE_GONE:
            case LEASE_LOST:
                try {
                    exec.awaitTermination(2, TimeUnit.MINUTES);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            break;
        }
    }

    public void processChanges(ChangeFeedObserverContext context, List<Document> docs) {

        if (exec == null)
            throw new NullPointerException("Exec is null, initiate the class properly before using it!");

        if (!(exec.isShutdown() || exec.isTerminated())) {
            Future future = exec.submit(() -> {
                for (Document d : docs) {
                    String content = d.toJson();
                    System.out.println("Received: " + content);
                }
            });

            try {
                future.get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }

    }
}
