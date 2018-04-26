package com.microsoft.azure.documentdb.changefeedprocessor;

import com.microsoft.azure.documentdb.changefeedprocessor.ChangeFeedObserverCloseReason;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

/**
*
* @author rogirdh
*/

final class PartitionObserverManager<T extends Lease> {
    final PartitionManager<T> partitionManager;
    final List<IPartitionObserver<T>> observers;
    private Logger logger = Logger.getLogger(PartitionObserverManager.class.getName());

    public PartitionObserverManager(PartitionManager<T> partitionManager){
        this.partitionManager = partitionManager;
        this.observers = new ArrayList<IPartitionObserver<T>>();
    }

    @SuppressWarnings({ "unchecked" })
    public Callable<AutoCloseable> subscribe(IPartitionObserver<T> observer) {
    	Callable<AutoCloseable> subscribeCallable = new Callable<AutoCloseable>() {
    		public AutoCloseable call() {
    			if (!PartitionObserverManager.this.observers.contains(observer)) {
    				PartitionObserverManager.this.observers.add(observer);
    		
    		        for (T lease : PartitionObserverManager.this.partitionManager.currentlyOwnedPartitions.values()) {	// CR: what "PartitionObserverManager." is here for?
    		            try {
    		                //  Wait till the runnable completes
    		            	observer.onPartitionAcquired(lease).wait();	// CR: who signals the wait to finish?
    		            } catch (Exception ex) {
    		                // Eat any exceptions during notification of observers
							logger.warning(ex.getMessage());
    		            }
    		        }
    		    }
				AutoCloseable unsubscriber = new Unsubscriber(PartitionObserverManager.this.observers, observer);
    		    return unsubscriber;
    		}
    	};	    
    	return subscribeCallable;
    }

    public Callable<Void> notifyPartitionAcquired(T lease) {
    	Callable<Void> notifyPartitionAcquiredRunnable = new Callable<Void>() {
    		@Override
    		public Void call() {
    			// CR: note that this complexity is not really needed as there is only 1 observer: change feed event host.
    			//     Could be done as simple as wait() like in subscribe method.
    			ExecutorService execSvc = Executors.newFixedThreadPool(PartitionObserverManager.this.observers.size());
    			List<Callable<Void>> oPA = new ArrayList<>();	// CR: rename, what is OPA BTW? :)
    			for (IPartitionObserver<T> obs : PartitionObserverManager.this.observers) {
    				oPA.add(obs.onPartitionAcquired(lease));  //TODO: Check if this works as expected. If not, change the return type of onPartitionAcquired to callable
    	        }
    			try {
					execSvc.invokeAll(oPA).stream().forEach((f) -> {
						try {
							f.get();
						} catch (InterruptedException e) {
							e.printStackTrace();
						} catch (ExecutionException e) {
							e.printStackTrace();
						}
					});	//TODO: Ensure this waits for executors to finish
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
    			execSvc.shutdown();
    			
    			return null;
    		}
    	};    	
    	return notifyPartitionAcquiredRunnable;      
    }

    public Callable<Void> notifyPartitionReleased(T lease, ChangeFeedObserverCloseReason reason) {
    	Callable<Void> notifyPartitionReleasedRunnable = new Callable<Void>() {
    		@Override
    		public Void call() {
    			ExecutorService execSvc = Executors.newFixedThreadPool(PartitionObserverManager.this.observers.size());
    			List<Callable<Void>> oPR = new ArrayList<>();
    			for (IPartitionObserver<T> obs : PartitionObserverManager.this.observers) {
    				oPR.add(obs.onPartitionReleased(lease, reason));  //TODO: Check if this works as expected. If not, change the return type of onPartitionAcquired to callable
    	        }
    			try {
					execSvc.invokeAll(oPR).stream().forEach((f)->{
						try {
							f.get();
						} catch (InterruptedException e) {
							e.printStackTrace();
						} catch (ExecutionException e) {
							e.printStackTrace();
						}
					});	//TODO: Ensure this waits for executors to finish
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
    			execSvc.shutdown();
    			
    			return null;
    		}
    	};    	
    	return notifyPartitionReleasedRunnable;
    }
}

