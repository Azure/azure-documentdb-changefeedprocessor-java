package com.microsoft.azure.documentdb.changefeedprocessor.internal;

import com.microsoft.azure.documentdb.changefeedprocessor.ChangeFeedObserverCloseReason;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
*
* @author rogirdh
*/

final class PartitionObserverManager<T extends Lease> {
    final PartitionManager<T> partitionManager;
    final List<IPartitionObserver<T>> observers;

    public PartitionObserverManager(PartitionManager<T> partitionManager){
        this.partitionManager = partitionManager;
        this.observers = new ArrayList<IPartitionObserver<T>>();
    }

    // TODO: implement subscribeAsync ,notifyPartitionAcquiredAsync,  notifyPartitionReleasedAsync

    @SuppressWarnings({ "unchecked" })
    public Callable<IDisposable> subscribe(IPartitionObserver<T> observer){
    	Callable<IDisposable> subscribeCallable = new Callable<IDisposable>() {
    		public IDisposable call() {
    			if (!PartitionObserverManager.this.observers.contains(observer)) {
    				PartitionObserverManager.this.observers.add(observer);
    		
    		        for (T lease : PartitionObserverManager.this.partitionManager.currentlyOwnedPartitions.values()) {
    		            try {
    		              //  Wait till the runnable completes
    		            	observer.onPartitionAcquired(lease).wait();
    		            } catch (Exception ex) {
    		                // Eat any exceptions during notification of observers
    		                TraceLog.exception(ex);
    		            }
    		        }
    		    }

    		    
				IDisposable unsubscriber = new Unsubscriber(PartitionObserverManager.this.observers, observer);
    		    return unsubscriber;
    		}
    	};	    
    	return subscribeCallable;
    }

    public Runnable notifyPartitionAcquired(T lease){
    	Runnable notifyPartitionAcquiredRunnable = new Runnable() {
    		public void run() {
    			ExecutorService execSvc = Executors.newFixedThreadPool(PartitionObserverManager.this.observers.size());
    			List<Callable<Object>> oPA = new ArrayList<Callable<Object>>();
    			for (IPartitionObserver<T> obs : PartitionObserverManager.this.observers) {
    				oPA.add(Executors.callable(obs.onPartitionAcquired(lease)));  //TODO: Check if this works as expected. If not, change the return type of onPartitionAcquired to callable
    	        }
    			try {
					execSvc.invokeAll(oPA);	//TODO: Ensure this waits for executors to finish
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
    			execSvc.shutdown();
    		}
    	};    	
    	return notifyPartitionAcquiredRunnable;      
    }

    public Runnable notifyPartitionReleased(T lease, ChangeFeedObserverCloseReason reason){
    	Runnable notifyPartitionReleasedRunnable = new Runnable() {
    		public void run() {
    			ExecutorService execSvc = Executors.newFixedThreadPool(PartitionObserverManager.this.observers.size());
    			List<Callable<Object>> oPR = new ArrayList<Callable<Object>>();
    			for (IPartitionObserver<T> obs : PartitionObserverManager.this.observers) {
    				oPR.add(Executors.callable(obs.onPartitionReleased(lease, reason)));  //TODO: Check if this works as expected. If not, change the return type of onPartitionAcquired to callable
    	        }
    			try {
					execSvc.invokeAll(oPR);	//TODO: Ensure this waits for executors to finish
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
    			execSvc.shutdown();
    		}
    	};    	
    	return notifyPartitionReleasedRunnable;
    }
}

