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

import com.microsoft.azure.documentdb.changefeedprocessor.ChangeFeedObserverCloseReason;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

final class PartitionObserverManager<T extends Lease> {
    final PartitionManager<T> partitionManager;
    final List<IPartitionObserver<T>> observers;
    private Logger logger = Logger.getLogger(PartitionObserverManager.class.getName());

    public PartitionObserverManager(PartitionManager<T> partitionManager){
        this.partitionManager = partitionManager;
        this.observers = new ArrayList<IPartitionObserver<T>>();
    }

    @SuppressWarnings({ "unchecked" })
    public Callable<AutoCloseable> subscribe(IPartitionObserver<T> observer){
    	Callable<AutoCloseable> subscribeCallable = new Callable<AutoCloseable>() {
    		public AutoCloseable call() {
    			if (!PartitionObserverManager.this.observers.contains(observer)) {
    				PartitionObserverManager.this.observers.add(observer);
    		
    		        for (T lease : PartitionObserverManager.this.partitionManager.currentlyOwnedPartitions.values()) {
    		            try {
    		              //  Wait till the runnable completes
    		            	observer.onPartitionAcquired(lease).wait();
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

    public Runnable notifyPartitionAcquired(T lease){
    	Runnable notifyPartitionAcquiredRunnable = new Runnable() {
    		public void run() {
    			ExecutorService execSvc = Executors.newFixedThreadPool(PartitionObserverManager.this.observers.size());
    			List<Callable<Void>> oPA = new ArrayList<>();
    			for (IPartitionObserver<T> obs : PartitionObserverManager.this.observers) {
    				oPA.add(obs.onPartitionAcquired(lease));
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
					});
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
    			List<Callable<Void>> oPR = new ArrayList<>();
    			for (IPartitionObserver<T> obs : PartitionObserverManager.this.observers) {
    				oPR.add(obs.onPartitionReleased(lease, reason));
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
					});
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
    			execSvc.shutdown();
    		}
    	};    	
    	return notifyPartitionReleasedRunnable;
    }
}

