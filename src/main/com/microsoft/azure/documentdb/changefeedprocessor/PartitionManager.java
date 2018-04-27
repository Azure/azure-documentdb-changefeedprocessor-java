/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
//package com.microsoft.azure.documentdb.changefeedprocessor.internal;
package com.microsoft.azure.documentdb.changefeedprocessor;

import com.microsoft.azure.documentdb.changefeedprocessor.ChangeFeedHostOptions;
import com.microsoft.azure.documentdb.changefeedprocessor.ChangeFeedObserverCloseReason;
import com.microsoft.azure.documentdb.changefeedprocessor.services.ConcurrentHashBag;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 *
 * @author rogirdh
 */
final class PartitionManager<T extends Lease> {

    final String workerName;
    final ILeaseManager<T> leaseManager;
    final ChangeFeedHostOptions options;
    final ConcurrentHashMap<String, T> currentlyOwnedPartitions;
    final ConcurrentHashMap<String, T> keepRenewingDuringClose;
    final PartitionObserverManager partitionObserverManager;
    private AtomicInteger isStarted  =new AtomicInteger(0);
    boolean shutdownComplete;	// CR: isShutdownComplete, for consistency with isStarted.
	private Future<Void> renewTask;
	private Future<Void> takerTask;
	private Logger logger = Logger.getLogger(PartitionManager.class.getName());
	private ExecutorService exec;	// CR: rename to something more meaningful.

    public PartitionManager(String workerName, ILeaseManager<T> leaseManager, ChangeFeedHostOptions options)
    {
    	// CR: asserts for input validation.
        this.workerName = workerName;
        this.leaseManager = leaseManager;
        this.options = options;

        this.currentlyOwnedPartitions = new ConcurrentHashMap<String, T>();
        this.keepRenewingDuringClose = new ConcurrentHashMap<String, T>();
        this.partitionObserverManager = new PartitionObserverManager(this);
    }

    public void initialize() throws Exception {
    	exec = Executors.newCachedThreadPool();
		initialize(exec);
    }
    
    private void initialize(ExecutorService execService) throws Exception {
        List<T> leases = new ArrayList<T>();	// CR: rename to more specific, it not clear what kind of leases these are.
        List<T> allLeases = new ArrayList<T>();

		logger.info(String.format("Host '%s' starting renew leases assigned to this host on initialize.", this.workerName));

        for (T lease : this.leaseManager.listLeases().call()) {
            allLeases.add(lease);
            if (lease.getOwner() == null || lease.getOwner().isEmpty() || lease.getOwner().equalsIgnoreCase(this.workerName)) {
            	Future<T> newLeaseFuture = execService.submit(this.renewLease(lease));
            	T newLease = newLeaseFuture.get();		//TODO: Ensure that this is not blocking other threads in the for loop	// CR: is this done?
                if (newLease != null) {
                    leases.add(newLease);
                } else {
					logger.info(String.format("Host '%s' unable to renew lease '%s' on startup.", this.workerName, lease.getPartitionId()));
                }
            }
        }

        List<Callable<Void>> addLeaseTasks = new ArrayList<Callable<Void>>();
        for (T lease : leases) {
            logger.info(String.format("Host '%s' acquired lease for PartitionId '%s' on startup.", this.workerName, lease.getPartitionId()));
            addLeaseTasks.add(this.addLease(lease));
        }

        try {
			execService.invokeAll(addLeaseTasks).forEach((f) -> {
				try {
					f.get();
				} catch (InterruptedException e) {
					e.printStackTrace();
				} catch (ExecutionException e) {
					e.printStackTrace();
				}
			});	//Wait till all tasks are finished
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
    	// CR: what will happen if one of the tasks throws DocumentClientException?
    }
    
    // [Done] call it when host starting
    // CR: important: this is never called! Should be called from initializeIntegrations...
    public void start()
    {
        if (!this.isStarted.compareAndSet(0, 1))
        {
            throw new IllegalStateException("PartitionManager has already started");
        }
        
        this.shutdownComplete = false;
  //      this.leaseTakerCancellationTokenSource = new CancellationTokenSource();
  //      this.leaseRenewerCancellationTokenSource = new CancellationTokenSource();

        this.renewTask = exec.submit(new LeaseRenewer());
        this.takerTask = exec.submit(new LeaseTaker());
    }

    // [Done] call it when the host shutdown 
    // CR: important: this is never called as well.
    public void stop(ChangeFeedObserverCloseReason reason) throws InterruptedException, ExecutionException {
        if (!this.isStarted.compareAndSet(1, 0)) {
            // idempotent
            return;
        }

        // CR: must test shutdown scenario.
        if (this.takerTask != null) {
            this.takerTask.cancel(true); //TODO: Ensure this is equivalent to cancellationtokensource i.e. the thread gets interrupted when this is called.
            //this.takerTask.get();	// CR: this may throw Interrupted exception that should be expected? Same below.
        }

        this.shutdown(reason);	//TODO: Equivalent to await?
        this.shutdownComplete = true;

        if (this.renewTask != null) {
            this.renewTask.cancel(true); //TODO: Ensure this is equivalent to cancellationtokensource i.e. the thread gets interrupted when this is called.
            //this.renewTask.get();	//TODO: Equivalent to await?
        }

     //   this.leaseTakerCancellationTokenSource = null;
     //   this.leaseRenewerCancellationTokenSource = null;
    }

    @SuppressWarnings("unchecked")
	public Callable<AutoCloseable> subscribe(IPartitionObserver<T> observer) {
        return this.partitionObserverManager.subscribe(observer);
    }

    public Callable<Void> tryReleasePartition(String partitionId, boolean hasOwnership, ChangeFeedObserverCloseReason closeReason) {
        Callable<Void> callable = new Callable<Void>() {
        	@Override
        	public Void call() throws Exception {
        		T lease = PartitionManager.this.currentlyOwnedPartitions.get(partitionId);
                
                if (lease != null) {
                	PartitionManager.this.removeLease(lease, hasOwnership, closeReason).call();	//TODO: Ensure this is awaiting
                }             
                return null;
        	}
        };
        
        return callable;
    }

    private class LeaseRenewer implements Callable<Void> {
		@Override
    	public Void call() throws Exception {
			while (PartitionManager.this.isStarted.intValue() == 1 || !PartitionManager.this.shutdownComplete) {
				try {
					logger.info(String.format("Host '%s' starting renewal of Leases.", PartitionManager.this.workerName));
	        		
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
							logger.warning(String.format("Host '%s' Renewed lease %s but failed to update it in the map (ignorable).", PartitionManager.this.workerName, lease));
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

				}  catch (Exception ex){
					logger.info(ex.getMessage());
				}
				
				TimeUnit.SECONDS.sleep(PartitionManager.this.options.getLeaseRenewInterval().getSeconds()); //TODO: Not calling the CancellationTokenSource here. Will have to test to ensure this gets cancelled                	
				if(renewTask.isCancelled()){	//Checking to see if the task got cancelled
					logger.info(String.format("Host '%s' Renewer task canceled.", PartitionManager.this.workerName));
					break;
				}	
				// [Done] CR: eating exceptions: proaby should break out of the loop when getting cancellation/interrupted exception?
			}
			return null;
		}
    }
			
    private class LeaseTaker implements Callable<Void> {
		public Void call() throws Exception {
			while (PartitionManager.this.isStarted.intValue()==1){
				 try
	            {
					logger.info(String.format("Host '%s' starting to check for available leases.", PartitionManager.this.workerName));
	                HashMap<String, T> availableLeases = PartitionManager.this.takeLeases();	//TODO Change to Callable if synchronous doesn't work
	                int i = availableLeases.size();
	                if (i > 0)
						logger.info(String.format("Host '%s' adding %d leases...", PartitionManager.this.workerName, i));

	                List<Callable<Void>> addLeaseTasks = new ArrayList<Callable<Void>>();
	                for (T kvp : availableLeases.values()) {
	                    addLeaseTasks.add(PartitionManager.this.addLease(kvp));
	                }

	        		exec.invokeAll(addLeaseTasks);	//Waits till all tasks are finished
	            }
	            catch (Exception ex) {
					ex.printStackTrace();
					logger.warning(ex.getMessage());
	            }

				TimeUnit.SECONDS.sleep(PartitionManager.this.options.getLeaseAcquireInterval().getSeconds()); //TODO: Not calling the CancellationTokenSource here. Will have to test to ensure this gets cancelled                	
				if(takerTask.isCancelled()){
					logger.info(String.format("Host '%s' AcquireLease task canceled.", PartitionManager.this.workerName));
					break;
				}
					// [Done] CR: should we break out of the loop?
	        }
			logger.info(String.format("Host '%s' AcquireLease task completed.", PartitionManager.this.workerName));
			return null;
		}
    }

    private HashMap<String, T> takeLeases() throws Exception { //Setting this method up to be sync, since it is being called from a thread that needs to await the result of this method. TODO: Test for performance
    	HashMap<String, T> allPartitions = new HashMap<String, T>();
        HashMap<String, T> takenLeases = new HashMap<String, T>();
        HashMap<String, Integer> workerToPartitionCount = new HashMap<String, Integer>();
        List<T> expiredLeases = new ArrayList<T>();

        for (T lease : this.leaseManager.listLeases().call()) {
            assert lease.getPartitionId() != null : "takeLeases: lease.PartitionId cannot be null.";

            allPartitions.put(lease.getPartitionId(), lease);
            if (isNullOrWhitespace(lease.getOwner()) || this.leaseManager.isExpired(lease).call()){
				logger.info(String.format("Found unused or expired lease: %s", lease));
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
            
			logger.info(
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

						logger.info(String.format("Host '%s' attempting to take lease for PartitionId '%s'.", this.workerName, leaseToTake.getPartitionId()));
                        T acquiredLease;
						try {
							acquiredLease = exec.submit(this.tryAcquireLease(leaseToTake)).get();
							if (acquiredLease != null) {
								logger.info(String.format("Host '%s' successfully acquired lease for PartitionId '%s': %s", this.workerName, leaseToTake.getPartitionId(), acquiredLease));
	                            takenLeases.put(acquiredLease.getPartitionId(), acquiredLease);
	                            partitionsNeededForMe--;
	                        }
						} catch (InterruptedException e) {
							// CR: why catch interrupted exception here? We already catch it in the caller and can react there.
							//     If we eat it here, the caller cannot react.
							logger.info(String.format("Host '%s' was unable to acquire lease for PartitionId '%s'", this.workerName, leaseToTake.getPartitionId()));
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
								logger.info(String.format("Host '%s' attempting to steal lease from '%s' for PartitionId '%s'.", this.workerName, workerToStealFrom.getKey(), leaseToTake.getPartitionId()));
					            T stolenLease;
								try {
									stolenLease = exec.submit(this.tryStealLease(leaseToTake)).get();
									if (stolenLease != null) {
										logger.info(String.format("Host '%s' stole lease from '%s' for PartitionId '%s'.", this.workerName, workerToStealFrom.getKey(), leaseToTake.getPartitionId()));
						                takenLeases.put(stolenLease.getPartitionId(), stolenLease);
						
						                partitionsNeededForMe--;
						
						                // Only steal one lease at a time
						                break;
						            }
								} catch (InterruptedException | ExecutionException e) {
									logger.info(String.format("Host '%s' was unable to steal lease from '%s' for PartitionId '%s'.", this.workerName, workerToStealFrom.getKey(), leaseToTake.getPartitionId()));
								}
					        }
					    }
					}
                }
            }
        }

        return takenLeases;
    }

    private void shutdown(ChangeFeedObserverCloseReason reason)
    {
        List<Callable<Void>> shutdownTasks = new ArrayList<Callable<Void>>();
        for(T value : this.currentlyOwnedPartitions.values()) {
        	shutdownTasks.add(this.removeLease(value, true, reason));
        }

        try {
			exec.invokeAll(shutdownTasks);	//Wait till all tasks are finished
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }

    Callable<T> renewLease(T lease)	
    {
    	Callable<T> callable = new Callable<T>() {
    		public T call() {
    			T renewedLease = null;
    	        try{
					logger.info(String.format("Host '%s' renewing lease for PartitionId '%s' with lease token '%s'", PartitionManager.this.workerName, lease.getPartitionId(), lease.getConcurrencyToken()));

					// CR: I don't get the awit logic for a single call. If submit blocks, there is no reason to use separate thread.
					//     Not specific to this place only, but overall.
    	            renewedLease = exec.submit(PartitionManager.this.leaseManager.renew(lease)).get();
    	        }
    	        catch (LeaseLostException ex) {
					logger.info(String.format("Host '%s' got LeaseLostException trying to renew lease for  PartitionId '%s' with lease token '%s'", PartitionManager.this.workerName, lease.getPartitionId(), lease.getConcurrencyToken()));
    	        }
    	        catch (Exception ex) {
					logger.warning(ex.getMessage());

    	            // Eat any exceptions during renew and keep going.
    	            // Consider the lease as renewed.  Maybe lease store outage is causing the lease to not get renewed.
    	            renewedLease = lease;
    	        }
    	        finally {
					logger.info(String.format("Host '%s' attempted to renew lease for PartitionId '%s' and lease token '%s' with result: '%s'", PartitionManager.this.workerName, lease.getPartitionId(), lease.getConcurrencyToken(), renewedLease != null));
    	        }

    	        return renewedLease;
    		}
    	};
    	
        return callable;
    }

    Callable<T> tryAcquireLease(T lease){
        Callable<T> callable = new Callable<T>() {
        	public T call() {
        		try {
                    return exec.submit(PartitionManager.this.leaseManager.acquire(lease, PartitionManager.this.workerName)).get() ;
                } catch (LeaseLostException ex) {
					logger.info(String.format("Host '%s' failed to acquire lease for PartitionId '%s' due to conflict.", PartitionManager.this.workerName, lease.getPartitionId()));
                } catch (Exception ex) {
                    // Eat any exceptions during acquiring lease.
					logger.warning(ex.getMessage());
                }
                return null;
        	}
        };  	
    	return callable;
    }

    Callable<T> tryStealLease(T lease){
    	Callable<T> callable = new Callable<T>() {
        	public T call() {
        		try {
                    return exec.submit(PartitionManager.this.leaseManager.acquire(lease, PartitionManager.this.workerName)).get() ;
                } catch (LeaseLostException ex){
		            // Concurrency issue in stealing the lease, someone else got it before us
					logger.info(String.format("Host '%s' failed to steal lease for PartitionId '%s' due to conflict.", PartitionManager.this.workerName, lease.getPartitionId()));
		        } catch (Exception ex) {
		            // Eat any exceptions during stealing
					logger.warning(ex.getMessage());
		        }
        		return null;
        	}
    	};
    	return callable;
    }

    Callable<Void> addLease(T lease){
    	Callable<Void> addLeaseRunnable = new Callable<Void>() {
    		@Override
    		public Void call() {
    			if (!PartitionManager.this.currentlyOwnedPartitions.containsKey(lease.getPartitionId())) {	//Replacing TryAdd method for Dictionary in C#
    				PartitionManager.this.currentlyOwnedPartitions.put(lease.getPartitionId(), lease);
    	            boolean failedToInitialize = false;
    	            try {
						logger.info(String.format("Host '%s' opening event processor for PartitionId '%s' and lease token '%s'", PartitionManager.this.workerName, lease.getPartitionId(), lease.getConcurrencyToken()));

    	                PartitionManager.this.partitionObserverManager.notifyPartitionAcquired(lease).call();	//TODO: Make notifyPartitionAcquired async and add await equivalent?

						logger.info(String.format("Host '%s' opened event processor for PartitionId '%s' and lease token '%s'", PartitionManager.this.workerName, lease.getPartitionId(), lease.getConcurrencyToken()));
    	            }
    	            catch (Exception ex) {
						logger.info(String.format("Host '%s' failed to initialize processor for PartitionId '%s' and lease token '%s'", PartitionManager.this.workerName, lease.getPartitionId(), lease.getConcurrencyToken()));
    	                failedToInitialize = true;

    	                // Eat any exceptions during notification of observers
						logger.warning(ex.getMessage());
    	            }

    	            // We need to release the lease if we fail to initialize the processor, so some other node can pick up the partition
    	            if (failedToInitialize) {
    	            	PartitionManager.this.removeLease(lease, true, ChangeFeedObserverCloseReason.OBSERVER_ERROR);	//TODO: await equivalent
    	            }
    	        }
    	        else {
    	            // We already acquired lease for this partition but it looks like we previously owned this partition
    	            // and haven't completed the shutdown process for it yet.  Release lease for possible others hosts to
    	            // pick it up.
    	            try {
						logger.warning(String.format("Host '%s' unable to add PartitionId '%s' with lease token '%s' to currently owned partitions.", PartitionManager.this.workerName, lease.getPartitionId(), lease.getConcurrencyToken()));

    	                PartitionManager.this.leaseManager.release(lease);	//TODO: await equivalent

						logger.info(String.format("Host '%s' successfully released lease on PartitionId '%s' with lease token '%s'", PartitionManager.this.workerName, lease.getPartitionId(), lease.getConcurrencyToken()));
    	            }
    	            catch (LeaseLostException ex){
    	                // We have already shutdown the processor so we can ignore any LeaseLost at this point
						logger.info(String.format("Host '%s' failed to release lease for PartitionId '%s' with lease token '%s' due to conflict.", PartitionManager.this.workerName, lease.getPartitionId(), lease.getConcurrencyToken()));
    	            }
    	            catch (Exception ex) {
						logger.warning(ex.getMessage());
    	            }
    	        }
    			
    			return null;
    		}
    	};
        
        
        return addLeaseRunnable;
    }

    Callable<Void> removeLease(T lease, boolean hasOwnership, ChangeFeedObserverCloseReason closeReason){
    	
    	Callable<Void> removeLeaseRunnable = new Callable<Void>() {
    		@Override
    		public Void call() {
    			T thisLease = lease;
    			if (thisLease != null && PartitionManager.this.currentlyOwnedPartitions != null){
    	        	
    	        	String partitionId = thisLease.getPartitionId();
    	        	thisLease = PartitionManager.this.currentlyOwnedPartitions.get(partitionId);
    	        	
    	        	if (thisLease != null) {
    	        		PartitionManager.this.currentlyOwnedPartitions.remove(partitionId);

						logger.info(String.format("Host '%s' successfully removed PartitionId '%s' with lease token '%s' from currently owned partitions.", PartitionManager.this.workerName, thisLease.getPartitionId(), thisLease.getConcurrencyToken()));

    	                try {
    	                    if (hasOwnership) {
    	                    	PartitionManager.this.keepRenewingDuringClose.put(thisLease.getPartitionId(), thisLease);
    	                    }

							logger.info(String.format("Host '%s' closing event processor for PartitionId '%s' and lease token '%s' with reason '%s'", PartitionManager.this.workerName, thisLease.getPartitionId(), thisLease.getConcurrencyToken(), closeReason));

    	                    // Notify the host that we lost partition so shutdown can be triggered on the host
    	                    PartitionManager.this.partitionObserverManager.notifyPartitionReleased(thisLease, closeReason);

							logger.info(String.format("Host '%s' closed event processor for PartitionId '%s' and lease token '%s' with reason '%s'", PartitionManager.this.workerName, thisLease.getPartitionId(), thisLease.getConcurrencyToken(), closeReason));
    	                }
    	                catch (Exception ex) {
    	                    // Eat any exceptions during notification of observers
							logger.warning(ex.getMessage());
    	                }
    	                finally {
    	                    if (hasOwnership) {
    	                    	thisLease = PartitionManager.this.keepRenewingDuringClose.get(thisLease.getPartitionId());
    	                    	if (thisLease != null) {
    	                    		PartitionManager.this.keepRenewingDuringClose.remove(thisLease.getPartitionId());
    	                    	}   	
    	                    }
    	                }

    	                if (hasOwnership) {
    	                    try
    	                    {
    	                    	PartitionManager.this.leaseManager.release(thisLease);	//TODO: Equivalent to await?
								logger.info(String.format("Host '%s' successfully released lease on PartitionId '%s' with lease token '%s'", PartitionManager.this.workerName, thisLease.getPartitionId(), thisLease.getConcurrencyToken()));
    	                    }
    	                    catch (LeaseLostException ex)
    	                    {
    	                        // We have already shutdown the processor so we can ignore any LeaseLost at this point
								logger.info(String.format("Host '%s' failed to release lease for PartitionId '%s' with lease token '%s' due to conflict.", PartitionManager.this.workerName, thisLease.getPartitionId(), thisLease.getConcurrencyToken()));
    	                    }
    	                    catch (Exception ex)
    	                    {
								logger.warning(ex.getMessage());
    	                    }
    	                }
    	        		
    	        	}
    	        }
    			
    			return null;
    		}
    	};
    	
    	return removeLeaseRunnable;
    }
    
    private static boolean isNullOrWhitespace(String s) {
        return s == null || isWhitespace(s);
    }
    
    // CR: (minor) this by itself is not needed, logic can be simplified if we move the code into isNullOrWhitespace.
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
