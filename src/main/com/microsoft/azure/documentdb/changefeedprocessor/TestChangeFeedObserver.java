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
                //TODO: I think that for some case we should wait the current work to be done. To be discussed with Gafa
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
