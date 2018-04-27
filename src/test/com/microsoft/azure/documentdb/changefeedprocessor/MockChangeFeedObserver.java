package com.microsoft.azure.documentdb.changefeedprocessor;

import com.microsoft.azure.documentdb.Document;

import java.util.List;
import java.util.concurrent.*;

import org.junit.Test;

public class MockChangeFeedObserver implements ChangeFeedObserverInterface {

    private ExecutorService exec = null;

    public  MockChangeFeedObserver(){
        exec = Executors.newFixedThreadPool(1);

    }

    public Callable<Void> open(ChangeFeedObserverContext context) {
    	return null;	// TODO: fix this
    }

    public Callable<Void> close(ChangeFeedObserverContext context, ChangeFeedObserverCloseReason reason) {

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
        
        return null;	// TODO: fix this
    }

    public Callable<Void> processChanges(ChangeFeedObserverContext context, List<Document> docs) {

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
        return null;	// TODO: fix this
    }
}
