/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.microsoft.azure.documentdb.changefeedprocessor.internal;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.management.OperationsException;

import com.microsoft.azure.documentdb.changefeedprocessor.*;
import com.microsoft.azure.documentdb.changefeedprocessor.services.ConcurrentHashBag;

/**
 *
 * @author rogirdh
 */
public class PartitionManager<T extends Lease> {

    final String workerName;
    final ILeaseManager<T> leaseManager;
    final ChangeFeedHostOptions options;
    final ConcurrentHashMap<String, T> currentlyOwnedPartitions;
    final ConcurrentHashMap<String, T> keepRenewingDuringClose;
    final PartitionObserverManager partitionObserverManager;

    AtomicInteger isStarted;
    boolean shutdownComplete;
    Future<Void> renewTask;
    Future<Void> takerTask;
  //  CancellationTokenSource leaseTakerCancellationTokenSource;	  //Need CancellationTokenSource Java equivalent
  //  CancellationTokenSource leaseRenewerCancellationTokenSource;  //Need CancellationTokenSource Java equivalent
//    ExecutorService execService;

    public PartitionManager(String workerName, ILeaseManager<T> leaseManager, ChangeFeedHostOptions options)
    {
        this.workerName = workerName;
        this.leaseManager = leaseManager;
        this.options = options;

        this.currentlyOwnedPartitions = new ConcurrentHashMap<String, T>();
        this.keepRenewingDuringClose = new ConcurrentHashMap<String, T>();
        this.partitionObserverManager = new PartitionObserverManager(this);
    }
    
    private ExecutorService exec ;

    //Java
    public void initialize() throws Exception {
    	exec = Executors.newCachedThreadPool();
    	initialize(exec);
    }
    
    //Java
    private void initialize(ExecutorService execService) throws Exception {
        List<T> leases = new ArrayList<T>();
        List<T> allLeases = new ArrayList<T>();

        TraceLog.verbose(String.format("Host '%s' starting renew leases assigned to this host on initialize.", this.workerName));

        for (T lease : this.leaseManager.listLeases().call()) {
            allLeases.add(lease);
            if (lease.getOwner().equalsIgnoreCase(this.workerName)) {
            	Future<T> newLeaseFuture = execService.submit(this.renewLease(lease));
            	T newLease = newLeaseFuture.get();		//TODO: Ensure that this is not blocking other threads in the for loop
                if (newLease != null) {
                    leases.add(newLease);
                } else {
                    TraceLog.informational(String.format("Host '%s' unable to renew lease '%s' on startup.", this.workerName, lease.getPartitionId()));
                }
            }
        }

        List<Callable<Object>> addLeaseTasks = new ArrayList<Callable<Object>>();
        for (T lease : leases) {
            TraceLog.informational(String.format("Host '%s' acquired lease for PartitionId '%s' on startup.", this.workerName, lease.getPartitionId()));
            addLeaseTasks.add(Executors.callable(this.addLease(lease)));	//TODO: Ensure this converts Runnable to Callable safely
        }

        try {
			execService.invokeAll(addLeaseTasks);	//Wait till all tasks are finished
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

  }

    //Java
    public void startAsync()
    {
        if (!this.isStarted.compareAndSet(0, 1))
        {
            throw new IllegalStateException("Controller has already started");
        }
        

        this.shutdownComplete = false;
  //      this.leaseTakerCancellationTokenSource = new CancellationTokenSource();
  //      this.leaseRenewerCancellationTokenSource = new CancellationTokenSource();

        this.renewTask = exec.submit(new LeaseRenewerAsync());
        this.takerTask = exec.submit(new LeaseTakerAsync());
    }

    //Java
    public void stopAsync(ChangeFeedObserverCloseReason reason) throws InterruptedException, ExecutionException {
        if (!this.isStarted.compareAndSet(1, 0)) {
            // idempotent
            return;
        }

        if (this.takerTask != null) {
            this.takerTask.cancel(true); //TODO: Ensure this is equivalent to cancellationtokensource i.e. the thread gets interrupted when this is called.
            this.takerTask.get();	
        }

        this.shutdown(reason);	//TODO: Equivalent to await?
        this.shutdownComplete = true;

        if (this.renewTask != null) {
            this.renewTask.cancel(true); //TODO: Ensure this is equivalent to cancellationtokensource i.e. the thread gets interrupted when this is called.
            this.renewTask.get();	//TODO: Equivalent to await?
        }

     //   this.leaseTakerCancellationTokenSource = null;
     //   this.leaseRenewerCancellationTokenSource = null;
    }

    //Java
    @SuppressWarnings("unchecked")
	public Callable<IDisposable> subscribe(IPartitionObserver<T> observer) {
        return this.partitionObserverManager.subscribe(observer);
    }

    //Java
    public Callable<Void> tryReleasePartition(String partitionId, boolean hasOwnership, ChangeFeedObserverCloseReason closeReason) {
        Callable<Void> callable = new Callable<Void>() {
        	@Override
        	public Void call() {
        		T lease = PartitionManager.this.currentlyOwnedPartitions.get(partitionId);
                
                if (lease != null) {
                	PartitionManager.this.removeLease(lease, hasOwnership, closeReason).run();	//TODO: Ensure this is awaiting
                }             
                return null;
        	}
        };
        
        return callable;
    }

    //Java
    private class LeaseRenewerAsync implements Callable<Void> {
		@Override
    	public Void call() throws Exception {
			while (PartitionManager.this.isStarted.equals(1) || !PartitionManager.this.shutdownComplete) {
				try {
	        		TraceLog.informational(String.format("Host '%s' starting renewal of Leases.", PartitionManager.this.workerName));
	        		
	        		ConcurrentHashBag<T> renewedLeases = new ConcurrentHashBag<T>();
	        		ConcurrentHashBag<T> failedToRenewLeases = new ConcurrentHashBag<T>();
	                List<CompletableFuture<T>> renewTasks = new ArrayList<CompletableFuture<T>>();
	                
	                for (T lease : PartitionManager.this.currentlyOwnedPartitions.values()) {
	                	CompletableFuture<T> future = new CompletableFuture<T>();
	                	future = (CompletableFuture<T>) exec.submit(PartitionManager.this.renewLease(lease));
	                	future.whenComplete((renewResult, ex) -> {
	                		if (renewResult != null){
		                        renewedLeases.add(renewResult);
		                    } else {
		                        // Keep track of all failed attempts to renew so we can trigger shutdown for these partitions
		                        failedToRenewLeases.add(lease);
		                    }
	                	});
	                	renewTasks.add(future);
	                }
	                
	                List<T> failedToRenewShutdownLeases = new ArrayList<T>();
	                for(T shutdownLeases : PartitionManager.this.keepRenewingDuringClose.values()) {
	                	CompletableFuture<T> future = new CompletableFuture<T>();
	                	future = (CompletableFuture<T>) exec.submit(PartitionManager.this.renewLease(shutdownLeases));
	                	future.whenComplete((renewResult, ex) -> {
	                		if (renewResult != null){
		                        renewedLeases.add(renewResult);
		                    } else {
		                        // Keep track of all failed attempts to renew so we can trigger shutdown for these partitions
		                        failedToRenewLeases.add(shutdownLeases);
		                    }
	                	});
	                	renewTasks.add(future);
	                }
	                
	                try {
	        			CompletableFuture<Void> tasks = CompletableFuture.allOf(renewTasks.toArray(new CompletableFuture<?>[] {}));		//Adding all the CompletableFutures to a list
	        			tasks.get();	//Waiting for all tasks to complete
	        		} catch (Exception e) {
	        			// TODO Auto-generated catch block
	        			e.printStackTrace();
	        		}
	                
	             // Update renewed leases.
                    for (T lease : renewedLeases) {
                    	if(PartitionManager.this.currentlyOwnedPartitions.containsKey(lease.getPartitionId()) && PartitionManager.this.currentlyOwnedPartitions.get(lease.getPartitionId()).equals(lease)) {		//Replacing TryUpdate
                    		PartitionManager.this.currentlyOwnedPartitions.put(lease.getPartitionId(), lease);
                    	} else {
                    		TraceLog.warning(String.format("Host '%s' Renewed lease %s but failed to update it in the map (ignorable).", PartitionManager.this.workerName, lease));
                    	}
                    }
                    
                    //TODO: Start here
                 // Trigger shutdown of all partitions we failed to renew leases
                    failedToRenewLeases.forEach(lease -> exec.submit(PartitionManager.this.removeLease(lease, false, ChangeFeedObserverCloseReason.LEASE_LOST)));
                    

                    // Now remove all failed renewals of shutdown leases from further renewals
                    for (T failedToRenewShutdownLease : failedToRenewShutdownLeases)
                    {
                       // T removedLease = null;
                        PartitionManager.this.keepRenewingDuringClose.remove(failedToRenewShutdownLease.getPartitionId());
                    }

                    TimeUnit.SECONDS.sleep(PartitionManager.this.options.getLeaseRenewInterval().getSeconds()); //TODO: Not calling the CancellationTokenSource here. Will have to test to ensure this gets cancelled                	
                    
				} catch (CancellationException ex){
	                TraceLog.informational(String.format("Host '%s' Renewer task canceled.", PartitionManager.this.workerName));
	            } catch (Exception ex){
	                TraceLog.exception(ex);
	            }
				
			}
			return null;
		}
    }
			

   
    //Java
    private class LeaseTakerAsync implements Callable<Void> {
		public Void call() throws Exception {
			while (PartitionManager.this.isStarted.equals(1)){
	            try
	            {
	                TraceLog.informational(String.format("Host '%s' starting to check for available leases.", PartitionManager.this.workerName));
	                HashMap<String, T> availableLeases = PartitionManager.this.takeLeases();	//TODO Change to Callable if synchronous doesn't work
	                int i = availableLeases.size();
	                if (i > 0) 
	                	TraceLog.informational(String.format("Host '%s' adding %d leases...", PartitionManager.this.workerName, i));

	                List<Callable<Object>> addLeaseTasks = new ArrayList<Callable<Object>>();
	                for (T kvp : availableLeases.values()) {
	                    addLeaseTasks.add(Executors.callable(PartitionManager.this.addLease(kvp)));	//TODO: Ensure this wrapper of Runnable works
	                }

	                try {
	        			exec.invokeAll(addLeaseTasks);	//Waits till all tasks are finished
	        		} catch (InterruptedException e) {
	        			// TODO Auto-generated catch block
	        			e.printStackTrace();
	        		}
	            }
	            catch (Exception ex) {
	                TraceLog.exception(ex);
	            }

	            try {
	                TimeUnit.SECONDS.sleep(PartitionManager.this.options.getLeaseAcquireInterval().getSeconds()); //TODO: Not calling the CancellationTokenSource here. Will have to test to ensure this gets cancelled                	
	            }
	            catch (InterruptedException ex) {
	                TraceLog.informational(String.format("Host '%s' AcquireLease task canceled.", PartitionManager.this.workerName));
	            }
	        }

	        TraceLog.informational(String.format("Host '%s' AcquireLease task completed.", PartitionManager.this.workerName));
			return null;
		}
    }
    


    //Java
    HashMap<String, T> takeLeases() throws Exception { //Setting this method up to be sync, since it is being called from a thread that needs to await the result of this method. TODO: Test for performance
    	HashMap<String, T> allPartitions = new HashMap<String, T>();
        HashMap<String, T> takenLeases = new HashMap<String, T>();
        HashMap<String, Integer> workerToPartitionCount = new HashMap<String, Integer>();
        List<T> expiredLeases = new ArrayList<T>();

        for (T lease : this.leaseManager.listLeases().call()) {
            assert lease.getPartitionId() != null : "TakeLeasesAsync: lease.PartitionId cannot be null.";

            allPartitions.put(lease.getPartitionId(), lease);
            if (isNullOrWhitespace(lease.getOwner()) || this.leaseManager.isExpired(lease).call()){
                TraceLog.verbose(String.format("Found unused or expired lease: %s", lease));
                expiredLeases.add(lease);
            }
            else {
                int count = 0;
                String assignedTo = lease.getOwner();
                Integer tempCount = workerToPartitionCount.get(assignedTo);
                if (tempCount != null) {
                	count = tempCount;
                    workerToPartitionCount.put(assignedTo, new Integer(count + 1));
                }
                else {
                    workerToPartitionCount.put(assignedTo, 1);
                }
            }
        }

        if (!workerToPartitionCount.containsKey(this.workerName)) {
            workerToPartitionCount.put(this.workerName, 0);
        }

        int partitionCount = allPartitions.size();
        int workerCount = workerToPartitionCount.size();

        if (partitionCount > 0) {
            int target = 1;
            if (partitionCount > workerCount) {
                target = (int)Math.ceil((double)partitionCount / (double)workerCount);
            }

            assert this.options.getMinPartitionCount() <= this.options.getMaxPartitionCount();

            if (this.options.getMaxPartitionCount() > 0 && target > this.options.getMaxPartitionCount()) {
                target = this.options.getMaxPartitionCount();
            }

            if (this.options.getMinPartitionCount() > 0 && target < this.options.getMinPartitionCount()) {
                target = this.options.getMinPartitionCount();
            }

            int myCount = workerToPartitionCount.get(this.workerName);
            int partitionsNeededForMe = target - myCount;
            
            //TODO: Start Here
            TraceLog.informational(
                    String.format(
                            "Host '%s' %d partitions, %d hosts, %d available leases, target = %d, min = %d, max = %d, min = %d, will try to take %d lease(s) for myself'.",
                            this.workerName,
                            partitionCount,
                            workerCount,
                            expiredLeases.size(),
                            target,
                            this.options.getMinPartitionCount(),
                            this.options.getMaxPartitionCount(),
                            myCount,
                            Math.max(partitionsNeededForMe, 0)));

            if (partitionsNeededForMe > 0) {
                //HashSet<T> partitionsToAcquire = new HashSet<T>(); //->unused 
                if (expiredLeases.size() > 0) {
                    for (T leaseToTake : expiredLeases) {
                        if (partitionsNeededForMe == 0) {
                            break;
                        }

                        TraceLog.informational(String.format("Host '%s' attempting to take lease for PartitionId '%s'.", this.workerName, leaseToTake.getPartitionId()));
                        T acquiredLease;
						try {
							acquiredLease = exec.submit(this.tryAcquireLease(leaseToTake)).get();
							if (acquiredLease != null) {
	                            TraceLog.informational(String.format("Host '%s' successfully acquired lease for PartitionId '%s': %s", this.workerName, leaseToTake.getPartitionId(), acquiredLease));
	                            takenLeases.put(acquiredLease.getPartitionId(), acquiredLease);
	                            partitionsNeededForMe--;
	                        }
						} catch (InterruptedException e) {
							TraceLog.informational(String.format("Host '%s' was unable to acquire lease for PartitionId '%s'", this.workerName, leaseToTake.getPartitionId()));
						}
                    }
               }
               else {
					Map.Entry<String, Integer> workerToStealFrom = new AbstractMap.SimpleEntry<String, Integer>(null, 0);
					for (Map.Entry<String, Integer> kvp : workerToPartitionCount.entrySet()) {
						if (kvp.equals(new AbstractMap.SimpleEntry<String, Integer>(null, 0)) || workerToStealFrom.getValue() < kvp.getValue()) {
							workerToStealFrom = kvp;
					    }
					}

					if (workerToStealFrom.getValue() > target - (partitionsNeededForMe > 1 ? 1 : 0)) {
					    for (Map.Entry<String, T> kvp : allPartitions.entrySet()) {
					        if (kvp.getValue().getOwner().equalsIgnoreCase(workerToStealFrom.getKey())) {
					            T leaseToTake = kvp.getValue();
					            TraceLog.informational(String.format("Host '%s' attempting to steal lease from '%s' for PartitionId '%s'.", this.workerName, workerToStealFrom.getKey(), leaseToTake.getPartitionId()));
					            T stolenLease;
								try {
									stolenLease = exec.submit(this.tryStealLease(leaseToTake)).get();
									if (stolenLease != null) {
						                TraceLog.informational(String.format("Host '%s' stole lease from '%s' for PartitionId '%s'.", this.workerName, workerToStealFrom.getKey(), leaseToTake.getPartitionId()));
						                takenLeases.put(stolenLease.getPartitionId(), stolenLease);
						
						                partitionsNeededForMe--;
						
						                // Only steal one lease at a time
						                break;
						            }
								} catch (InterruptedException | ExecutionException e) {
									TraceLog.informational(String.format("Host '%s' was unable to steal lease from '%s' for PartitionId '%s'.", this.workerName, workerToStealFrom.getKey(), leaseToTake.getPartitionId()));
								}
					            
					        }
					    }
					}
                }
            }
        }

        return takenLeases;
    }

    //Java
    void shutdown(ChangeFeedObserverCloseReason reason)
    {
        List<Callable<Object>> shutdownTasks = new ArrayList<Callable<Object>>();
        for(T value : this.currentlyOwnedPartitions.values()) {
        	shutdownTasks.add(Executors.callable(this.removeLease(value, true, reason)));	//TODO: Test this wrapper works. If it doesn't, change the list of Callable<Object> to Callable<Void>
        }

        try {
			exec.invokeAll(shutdownTasks);	//Wait till all tasks are finished
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }

    //Java
    Callable<T> renewLease(T lease)	
    {
    	Callable<T> callable = new Callable<T>() {
    		public T call() {
    			T renewedLease = null;
    	        try{
    	            TraceLog.informational(String.format("Host '%s' renewing lease for PartitionId '%d' with lease token '%s'", PartitionManager.this.workerName, lease.getPartitionId(), lease.getConcurrencyToken()));

    	            renewedLease = exec.submit(PartitionManager.this.leaseManager.renew(lease)).get();
    	        }
    	        catch (LeaseLostException ex){
    	            TraceLog.informational(String.format("Host '%s' got LeaseLostException trying to renew lease for  PartitionId '%d' with lease token '%s'", PartitionManager.this.workerName, lease.getPartitionId(), lease.getConcurrencyToken()));
    	        }
    	        catch (Exception ex){
    	            TraceLog.exception(ex);

    	            // Eat any exceptions during renew and keep going.
    	            // Consider the lease as renewed.  Maybe lease store outage is causing the lease to not get renewed.
    	            renewedLease = lease;
    	        }
    	        finally{
    	            TraceLog.informational(String.format("Host '%s' attempted to renew lease for PartitionId '%d' and lease token '%s' with result: '%s'", PartitionManager.this.workerName, lease.getPartitionId(), lease.getConcurrencyToken(), renewedLease != null));
    	        }

    	        return renewedLease;
    		}
    	};
    	
        return callable;
    }

    //Java
    Callable<T> tryAcquireLease(T lease){
        Callable<T> callable = new Callable<T>() {
        	public T call() {
        		try{
                    return exec.submit(PartitionManager.this.leaseManager.acquire(lease, PartitionManager.this.workerName)).get() ;
                } catch (LeaseLostException ex) {
                    TraceLog.informational(String.format("Host '%s' failed to acquire lease for PartitionId '%d' due to conflict.", PartitionManager.this.workerName, lease.getPartitionId()));
                } catch (Exception ex) {
                    // Eat any exceptions during acquiring lease.
                    TraceLog.exception(ex);
                }
                return null;
        	}
        };  	
    	return callable;
    }

    //Java
    Callable<T> tryStealLease(T lease){
    	Callable<T> callable = new Callable<T>() {
        	public T call() {
        		try{
                    return exec.submit(PartitionManager.this.leaseManager.acquire(lease, PartitionManager.this.workerName)).get() ;
                } catch (LeaseLostException ex){
		            // Concurrency issue in stealing the lease, someone else got it before us
		            TraceLog.informational(String.format("Host '%s' failed to steal lease for PartitionId '%d' due to conflict.", PartitionManager.this.workerName, lease.getPartitionId()));
		        } catch (Exception ex){
		            // Eat any exceptions during stealing
		            TraceLog.exception(ex);
		        }
        		return null;
        	}
    	};
    	return callable;
    }

    //Java
    Runnable addLease(T lease){
    	Runnable addLeaseRunnable = new Runnable() {
    		@Override
    		public void run() {
    			if (!PartitionManager.this.currentlyOwnedPartitions.containsKey(lease.getPartitionId())) {	//Replacing TryAdd method for Dictionary in C#
    				PartitionManager.this.currentlyOwnedPartitions.put(lease.getPartitionId(), lease);
    	            boolean failedToInitialize = false;
    	            try{
    	                TraceLog.informational(String.format("Host '%s' opening event processor for PartitionId '%d' and lease token '%s'", PartitionManager.this.workerName, lease.getPartitionId(), lease.getConcurrencyToken()));

    	                PartitionManager.this.partitionObserverManager.notifyPartitionAcquired(lease);	//TODO: Make notifyPartitionAcquired async and add await equivalent?

    	                TraceLog.informational(String.format("Host '%s' opened event processor for PartitionId '%d' and lease token '%s'", PartitionManager.this.workerName, lease.getPartitionId(), lease.getConcurrencyToken()));
    	            }
    	            catch (Exception ex){
    	                TraceLog.informational(String.format("Host '%s' failed to initialize processor for PartitionId '%d' and lease token '%s'", PartitionManager.this.workerName, lease.getPartitionId(), lease.getConcurrencyToken()));
    	                failedToInitialize = true;

    	                // Eat any exceptions during notification of observers
    	                TraceLog.exception(ex);
    	            }

    	            // We need to release the lease if we fail to initialize the processor, so some other node can pick up the parition
    	            if (failedToInitialize){
    	            	PartitionManager.this.removeLease(lease, true, ChangeFeedObserverCloseReason.OBSERVER_ERROR);	//TODO: await equivalent
    	            }
    	        }
    	        else{
    	            // We already acquired lease for this partition but it looks like we previously owned this partition
    	            // and haven't completed the shutdown process for it yet.  Release lease for possible others hosts to
    	            // pick it up.
    	            try{
    	                TraceLog.warning(String.format("Host '%s' unable to add PartitionId '%d' with lease token '%s' to currently owned partitions.", PartitionManager.this.workerName, lease.getPartitionId(), lease.getConcurrencyToken()));

    	                PartitionManager.this.leaseManager.release(lease);	//TODO: await equivalent

    	                TraceLog.informational(String.format("Host '%s' successfully released lease on PartitionId '%d' with lease token '%s'", PartitionManager.this.workerName, lease.getPartitionId(), lease.getConcurrencyToken()));
    	            }
    	            catch (LeaseLostException ex){
    	                // We have already shutdown the processor so we can ignore any LeaseLost at this point
    	                TraceLog.informational(String.format("Host '%s' failed to release lease for PartitionId '%d' with lease token '%s' due to conflict.", PartitionManager.this.workerName, lease.getPartitionId(), lease.getConcurrencyToken()));
    	            }
    	            catch (Exception ex) {
    	                TraceLog.exception(ex);
    	            }
    	        }
    		}
    	};
        
        
        return addLeaseRunnable;
    }

    //Java
    Runnable removeLease(T lease, boolean hasOwnership, ChangeFeedObserverCloseReason closeReason){
    	
    	Runnable removeLeaseRunnable = new Runnable() {
    		@Override
    		public void run() {
    			T thisLease = lease;
    			if (thisLease != null && PartitionManager.this.currentlyOwnedPartitions != null){
    	        	
    	        	String partitionId = thisLease.getPartitionId();
    	        	thisLease = PartitionManager.this.currentlyOwnedPartitions.get(partitionId);
    	        	
    	        	if(thisLease != null) {
    	        		PartitionManager.this.currentlyOwnedPartitions.remove(partitionId);
    	        		
    	        		TraceLog.informational(String.format("Host '%s' successfully removed PartitionId '%s' with lease token '%s' from currently owned partitions.", PartitionManager.this.workerName, thisLease.getPartitionId(), thisLease.getConcurrencyToken()));

    	                try{
    	                    if (hasOwnership){
    	                    	PartitionManager.this.keepRenewingDuringClose.put(thisLease.getPartitionId(), thisLease);
    	                    }

    	                    TraceLog.informational(String.format("Host '%s' closing event processor for PartitionId '%s' and lease token '%s' with reason '%s'", PartitionManager.this.workerName, thisLease.getPartitionId(), thisLease.getConcurrencyToken(), closeReason));

    	                    // Notify the host that we lost partition so shutdown can be triggered on the host
    	                    PartitionManager.this.partitionObserverManager.notifyPartitionReleased(thisLease, closeReason);

    	                    TraceLog.informational(String.format("Host '%s' closed event processor for PartitionId '%s' and lease token '%s' with reason '%s'", PartitionManager.this.workerName, thisLease.getPartitionId(), thisLease.getConcurrencyToken(), closeReason));
    	                }
    	                catch (Exception ex){
    	                    // Eat any exceptions during notification of observers
    	                    TraceLog.exception(ex);
    	                }
    	                finally{
    	                    if (hasOwnership){
    	                    	thisLease = PartitionManager.this.keepRenewingDuringClose.get(thisLease.getPartitionId());
    	                    	if(thisLease != null) {
    	                    		PartitionManager.this.keepRenewingDuringClose.remove(thisLease.getPartitionId());
    	                    	}   	
    	                    }
    	                }

    	                if (hasOwnership){
    	                    try
    	                    {
    	                    	PartitionManager.this.leaseManager.release(thisLease);	//TODO: Equivalent to await?
    	                        TraceLog.informational(String.format("Host '%s' successfully released lease on PartitionId '%s' with lease token '%s'", PartitionManager.this.workerName, thisLease.getPartitionId(), thisLease.getConcurrencyToken()));
    	                    }
    	                    catch (LeaseLostException ex)
    	                    {
    	                        // We have already shutdown the processor so we can ignore any LeaseLost at this point
    	                        TraceLog.informational(String.format("Host '%s' failed to release lease for PartitionId '%s' with lease token '%s' due to conflict.", PartitionManager.this.workerName, thisLease.getPartitionId(), thisLease.getConcurrencyToken()));
    	                    }
    	                    catch (Exception ex)
    	                    {
    	                        TraceLog.exception(ex);
    	                    }
    	                }
    	        		
    	        	}
    	        }
    		}
    	};
    	
    	return removeLeaseRunnable;
            
    }
    
    public static boolean isNullOrWhitespace(String s) {
        return s == null || isWhitespace(s);

    }
    private static boolean isWhitespace(String s) {
        int length = s.length();
        if (length > 0) {
            for (int i = 0; i < length; i++) {
                if (!Character.isWhitespace(s.charAt(i))) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

}
