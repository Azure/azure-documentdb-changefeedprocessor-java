/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.microsoft.azure.documentdb.changefeedprocessor.internal;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import javax.management.OperationsException;

import com.microsoft.azure.documentdb.changefeedprocessor.*;

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
    CancellationTokenSource leaseTakerCancellationTokenSource;
    CancellationTokenSource leaseRenewerCancellationTokenSource;
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
    public void initialize() throws InterruptedException, ExecutionException {
    	exec = Executors.newFixedThreadPool(10);
    	initialize(exec);
    }
    
    //Java
    private void initialize(ExecutorService execService) throws InterruptedException, ExecutionException
    {
        List<T> leases = new ArrayList<T>();
        List<T> allLeases = new ArrayList<T>();

        TraceLog.verbose(String.format("Host '%s' starting renew leases assigned to this host on initialize.", this.workerName));

        for (T lease : this.leaseManager.listLeases())
        {
            allLeases.add(lease);

            if (lease.getOwner().equalsIgnoreCase(this.workerName))
            {
            	Future<T> renewedLease = execService.submit(this.renewLease(lease));
            	T newLease = renewedLease.get();
                if (newLease != null)
                {
                    leases.add(newLease);
                }
                else
                {
                    TraceLog.informational(String.format("Host '%s' unable to renew lease '%s' on startup.", this.workerName, lease.getPartitionId()));
                }
            }
        }

        List<Callable<Void>> addLeaseTasks = new ArrayList<Callable<Void>>();
        for (T lease : leases)
        {
            TraceLog.informational(String.format("Host '%s' acquired lease for PartitionId '%s' on startup.", this.workerName, lease.getPartitionId()));
            addLeaseTasks.add(this.addLease(lease));
        }

        try {
			execService.invokeAll(addLeaseTasks);	//Wait till all tasks are finished
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

  }

    //Java
    public void start()
    {
        if (!this.isStarted.compareAndSet(0, 1))
        {
            throw new IllegalStateException("Controller has already started");
        }
        

        this.shutdownComplete = false;
        this.leaseTakerCancellationTokenSource = new CancellationTokenSource();
        this.leaseRenewerCancellationTokenSource = new CancellationTokenSource();

        this.renewTask = exec.submit(this.leaseRenewer());
        this.takerTask = exec.submit(this.leaseTaker());
    }

    //Java
    public void stop(ChangeFeedObserverCloseReason reason) throws InterruptedException, ExecutionException
    {
        if (!this.isStarted.compareAndSet(1, 0))
        {
            // idempotent
            return;
        }

        if (this.takerTask != null)
        {
            this.leaseTakerCancellationTokenSource.cancel();
            this.takerTask.get();	//TODO: Equivalent to await?
        }

        this.shutdown(reason);	//TODO: Equivalent to await?
        this.shutdownComplete = true;

        if (this.renewTask != null)
        {
            this.leaseRenewerCancellationTokenSource.cancel();
            this.renewTask.get();	//TODO: Equivalent to await?
        }

        this.leaseTakerCancellationTokenSource = null;
        this.leaseRenewerCancellationTokenSource = null;
    }

    //Java
    public IDisposable subscribe(IPartitionObserver<T> observer)
    {
        return this.partitionObserverManager.subscribe(observer);
    }

    //Java
    public void tryReleasePartition(String partitionId, boolean hasOwnership, ChangeFeedObserverCloseReason closeReason)
    {
        T lease = this.currentlyOwnedPartitions.get(partitionId);
        
        if (lease != null)
        {
            this.removeLease(lease, hasOwnership, closeReason);	 //TODO: Equivalent to await?
        }
    }

    
    Callable<Void> leaseRenewer(){
    	
        while (this.isStarted.equals(1) || !this.shutdownComplete){
        	try {
        		TraceLog.informational(String.format("Host '%s' starting renewal of Leases.", this.workerName));
        		
        		ConcurrentHashMap<T> renewedLeases = new ConcurrentHashMap<T>();
                ConcurrentBag<T> failedToRenewLeases = new ConcurrentBag<T>();
                
                List<Task> renewTasks = new List<Task>(); //Copy stuff from below
        	}
        	catch (OperationsException ex){
                TraceLog.informational(String.format("Host '%s' Renewer task canceled.", this.workerName));
            }
            catch (Exception ex){
                TraceLog.exception(ex);
            }
        }
        
        while (this.isStarted.equals(1) || !this.shutdownComplete){
            try{
                

                
                List<Task> renewTasks = new List<Task>();

                // Renew leases for all currently owned partitions in parallel
                foreach (T lease in this.currentlyOwnedPartitions.Values)
                {
                    renewTasks.Add(this.RenewLeaseAsync(lease).ContinueWith(renewResult =>
                            {
                    if (renewResult.Result != null)
                    {
                        renewedLeases.Add(renewResult.Result);
                    }
                    else
                    {
                        // Keep track of all failed attempts to renew so we can trigger shutdown for these partitions
                        failedToRenewLeases.Add(lease);
                    }
                            }));
                }

                // Renew leases for all partitions currently in shutdown
                List<T> failedToRenewShutdownLeases = new List<T>();
                foreach (T shutdownLeases in this.keepRenewingDuringClose.Values)
                {
                    renewTasks.Add(this.RenewLeaseAsync(shutdownLeases).ContinueWith(renewResult =>
                            {
                    if (renewResult.Result != null)
                    {
                        renewedLeases.Add(renewResult.Result);
                    }
                    else
                    {
                        // Keep track of all failed attempts to renew shutdown leases so we can remove them from further renew attempts
                        failedToRenewShutdownLeases.Add(shutdownLeases);
                    }
                            }));
                }

                // Wait for all renews to complete
                await Task.WhenAll(renewTasks.ToArray());

                // Update renewed leases.
                foreach (T lease in renewedLeases)
                {
                    bool updateResult = this.currentlyOwnedPartitions.TryUpdate(lease.PartitionId, lease, lease);
                    if (!updateResult)
                    {
                        TraceLog.Warning(String.Format("Host '{0}' Renewed lease {1} but failed to update it in the map (ignorable).", this.workerName, lease));
                    }
                }

                // Trigger shutdown of all partitions we failed to renew leases
                await failedToRenewLeases.ForEachAsync(
                    async lease => await this.RemoveLeaseAsync(lease, false, ChangeFeedObserverCloseReason.LeaseLost),
                    this.options.DegreeOfParallelism);

                // Now remove all failed renewals of shutdown leases from further renewals
                foreach (T failedToRenewShutdownLease in failedToRenewShutdownLeases)
                {
                    T removedLease = null;
                    this.keepRenewingDuringClose.TryRemove(failedToRenewShutdownLease.PartitionId, out removedLease);
                }

                await Task.Delay(this.options.LeaseRenewInterval, this.leaseRenewerCancellationTokenSource.Token);
            }
            catch (OperationCanceledException)
            {
                TraceLog.Informational(String.Format("Host '{0}' Renewer task canceled.", this.workerName));
            }
            catch (Exception ex)
            {
                TraceLog.Exception(ex);
            }
        }

        this.currentlyOwnedPartitions.Clear();
        this.keepRenewingDuringClose.Clear();
        TraceLog.Informational(String.Format("Host '{0}' Renewer task completed.", this.workerName));
  }

    
    Callable<Void> leaseTaker()
    {
        while (this.isStarted.equals(1)){
            try
            {
                TraceLog.informational(String.format("Host '%s' starting to check for available leases.", this.workerName));
                HashMap<String, T> availableLeases = exec.submit(this.takeLeases()).get();	//TODO await equivalent?
                int i = availableLeases.size();
                if (i > 0) 
                	TraceLog.informational(String.format("Host '%s' adding %d leases...", this.workerName, i));

                List<Callable<Void>> addLeaseTasks = new ArrayList<Callable<Void>>();
                for (T kvp : availableLeases.values())
                {
                    addLeaseTasks.add(this.addLease(kvp));
                }

                try {
        			exec.invokeAll(addLeaseTasks);	//Waits till all tasks are finished
        		} catch (InterruptedException e) {
        			// TODO Auto-generated catch block
        			e.printStackTrace();
        		}
            }
            catch (Exception ex)
            {
                TraceLog.exception(ex);
            }

            try
            {
                await Task.Delay(this.options.LeaseAcquireInterval, this.leaseTakerCancellationTokenSource.);
            }
            catch (OperationsException ex)
            {
                TraceLog.informational(String.format("Host '%s' AcquireLease task canceled.", this.workerName));
            }
        }

        TraceLog.informational(String.format("Host '%s' AcquireLease task completed.", this.workerName));
    }

    //IDictionary<String, T>
    Callable<HashMap<String, T>> takeLeases() // it RETURNS IDictionary
    {
//        IDictionary<String, T> allPartitions = new Dictionary<String, T>();
//        IDictionary<String, T> takenLeases = new Dictionary<String, T>();
//        IDictionary<String, int> workerToPartitionCount = new Dictionary<String, int>();
//        List<T> expiredLeases = new List<T>();
//
//        foreach (var lease in await this.leaseManager.ListLeases())
//        {
//            Debug.Assert(lease.PartitionId != null, "TakeLeasesAsync: lease.PartitionId cannot be null.");
//
//            allPartitions.Add(lease.PartitionId, lease);
//            if (String.IsNullOrWhiteSpace(lease.Owner) || await this.leaseManager.IsExpired(lease))
//            {
//                TraceLog.Verbose(String.Format("Found unused or expired lease: {0}", lease));
//                expiredLeases.Add(lease);
//            }
//                else
//            {
//                int count = 0;
//                String assignedTo = lease.Owner;
//                if (workerToPartitionCount.TryGetValue(assignedTo, out count))
//                {
//                    workerToPartitionCount[assignedTo] = count + 1;
//                }
//                else
//                {
//                    workerToPartitionCount.Add(assignedTo, 1);
//                }
//            }
//        }
//
//        if (!workerToPartitionCount.ContainsKey(this.workerName))
//        {
//            workerToPartitionCount.Add(this.workerName, 0);
//        }
//
//        int partitionCount = allPartitions.Count;
//        int workerCount = workerToPartitionCount.Count;
//
//        if (partitionCount > 0)
//        {
//            int target = 1;
//
//            if (partitionCount > workerCount)
//            {
//                target = (int)Math.Ceiling((double)partitionCount / (double)workerCount);
//            }
//
//            Debug.Assert(this.options.MinPartitionCount <= this.options.MaxPartitionCount);
//
//            if (this.options.MaxPartitionCount > 0 && target > this.options.MaxPartitionCount)
//            {
//                target = this.options.MaxPartitionCount;
//            }
//
//            if (this.options.MinPartitionCount > 0 && target < this.options.MinPartitionCount)
//            {
//                target = this.options.MinPartitionCount;
//            }
//
//            int myCount = workerToPartitionCount[this.workerName];
//            int partitionsNeededForMe = target - myCount;
//            TraceLog.Informational(
//                    String.Format(
//                            "Host '{0}' {1} partitions, {2} hosts, {3} available leases, target = {4}, min = {5}, max = {6}, mine = {7}, will try to take {8} lease(s) for myself'.",
//                            this.workerName,
//                            partitionCount,
//                            workerCount,
//                            expiredLeases.Count,
//                            target,
//                            this.options.MinPartitionCount,
//                            this.options.MaxPartitionCount,
//                            myCount,
//                            Math.Max(partitionsNeededForMe, 0)));
//
//            if (partitionsNeededForMe > 0)
//            {
//                HashSet<T> partitionsToAcquire = new HashSet<T>();
//                if (expiredLeases.Count > 0)
//                {
//                    foreach (T leaseToTake in expiredLeases)
//                    {
//                        if (partitionsNeededForMe == 0)
//                        {
//                            break;
//                        }
//
//                        TraceLog.Informational(String.Format("Host '{0}' attempting to take lease for PartitionId '{1}'.", this.workerName, leaseToTake.PartitionId));
//                        T acquiredLease = await this.TryAcquireLeaseAsync(leaseToTake);
//                        if (acquiredLease != null)
//                        {
//                            TraceLog.Informational(String.Format("Host '{0}' successfully acquired lease for PartitionId '{1}': {2}", this.workerName, leaseToTake.PartitionId, acquiredLease));
//                            takenLeases.Add(acquiredLease.PartitionId, acquiredLease);
//
//                            partitionsNeededForMe--;
//                        }
//                    }
//                }
//                else
//                {
//                    KeyValuePair<String, int> workerToStealFrom = default(KeyValuePair<String, int>);
//                    foreach (var kvp in workerToPartitionCount)
//                {
//                    if (kvp.Equals(default(KeyValuePair<String, int>)) || workerToStealFrom.Value < kvp.Value)
//                {
//                    workerToStealFrom = kvp;
//                }
//                }
//
//                if (workerToStealFrom.Value > target - (partitionsNeededForMe > 1 ? 1 : 0))
//                {
//                    foreach (var kvp in allPartitions)
//                    {
//                        if (String.Equals(kvp.Value.Owner, workerToStealFrom.Key, StringComparison.OrdinalIgnoreCase))
//                        {
//                            T leaseToTake = kvp.Value;
//                            TraceLog.Informational(String.Format("Host '{0}' attempting to steal lease from '{1}' for PartitionId '{2}'.", this.workerName, workerToStealFrom.Key, leaseToTake.PartitionId));
//                            T stolenLease = await this.TryStealLeaseAsync(leaseToTake);
//                            if (stolenLease != null)
//                            {
//                                TraceLog.Informational(String.Format("Host '{0}' stole lease from '{1}' for PartitionId '{2}'.", this.workerName, workerToStealFrom.Key, leaseToTake.PartitionId));
//                                takenLeases.Add(stolenLease.PartitionId, stolenLease);
//
//                                partitionsNeededForMe--;
//
//                                // Only steal one lease at a time
//                                break;
//                            }
//                        }
//                    }
//                }
//                }
//            }
//        }
//
//        return takenLeases;
        return null;
    }

    //Java
    void shutdown(ChangeFeedObserverCloseReason reason)
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
//        T renewedLease = null;
//        try
//        {
//            TraceLog.Informational(String.Format("Host '{0}' renewing lease for PartitionId '{1}' with lease token '{2}'", this.workerName, lease.PartitionId, lease.ConcurrencyToken));
//
//            renewedLease = await this.leaseManager.RenewAsync(lease);
//        }
//        catch (LeaseLostException)
//        {
//            TraceLog.Informational(String.Format("Host '{0}' got LeaseLostException trying to renew lease for  PartitionId '{1}' with lease token '{2}'", this.workerName, lease.PartitionId, lease.ConcurrencyToken));
//        }
//        catch (Exception ex)
//        {
//            TraceLog.Exception(ex);
//
//            // Eat any exceptions during renew and keep going.
//            // Consider the lease as renewed.  Maybe lease store outage is causing the lease to not get renewed.
//            renewedLease = lease;
//        }
//        finally
//        {
//            TraceLog.Informational(String.Format("Host '{0}' attempted to renew lease for PartitionId '{1}' and lease token '{2}' with result: '{3}'", this.workerName, lease.PartitionId, lease.ConcurrencyToken, renewedLease != null));
//        }
//
//        return renewedLease;
        return null;
    }

    T tryAcquireLease(T lease)
    {
//        try
//        {
//            return await this.leaseManager.AcquireAsync(lease, this.workerName);
//        }
//        catch (LeaseLostException)
//        {
//            TraceLog.Informational(String.Format("Host '{0}' failed to acquire lease for PartitionId '{1}' due to conflict.", this.workerName, lease.PartitionId));
//        }
//        catch (Exception ex)
//        {
//            // Eat any exceptions during acquiring lease.
//            TraceLog.Exception(ex);
//        }
//
//        return null;

        return null;
    }

    T tryStealLease(T lease)
    {
//        try
//        {
//            return await this.leaseManager.AcquireAsync(lease, this.workerName);
//        }
//        catch (LeaseLostException)
//        {
//            // Concurrency issue in stealing the lease, someone else got it before us
//            TraceLog.Informational(String.Format("Host '{0}' failed to steal lease for PartitionId '{1}' due to conflict.", this.workerName, lease.PartitionId));
//        }
//        catch (Exception ex)
//        {
//            // Eat any exceptions during stealing
//            TraceLog.Exception(ex);
//        }
//
//        return null;

        return null;
    }

    Callable<Void> addLease(T lease)
    {
		return null;
//        if (this.currentlyOwnedPartitions.TryAdd(lease.PartitionId, lease))
//        {
//            bool failedToInitialize = false;
//            try
//            {
//                TraceLog.Informational(String.Format("Host '{0}' opening event processor for PartitionId '{1}' and lease token '{2}'", this.workerName, lease.PartitionId, lease.ConcurrencyToken));
//
//                await this.partitionObserverManager.NotifyPartitionAcquiredAsync(lease);
//
//                TraceLog.Informational(String.Format("Host '{0}' opened event processor for PartitionId '{1}' and lease token '{2}'", this.workerName, lease.PartitionId, lease.ConcurrencyToken));
//            }
//            catch (Exception ex)
//            {
//                TraceLog.Informational(String.Format("Host '{0}' failed to initialize processor for PartitionId '{1}' and lease token '{2}'", this.workerName, lease.PartitionId, lease.ConcurrencyToken));
//
//                failedToInitialize = true;
//
//                // Eat any exceptions during notification of observers
//                TraceLog.Exception(ex);
//            }
//
//            // We need to release the lease if we fail to initialize the processor, so some other node can pick up the parition
//            if (failedToInitialize)
//            {
//                await this.RemoveLeaseAsync(lease, true, ChangeFeedObserverCloseReason.ObserverError);
//            }
//        }
//        else
//        {
//            // We already acquired lease for this partition but it looks like we previously owned this partition
//            // and haven't completed the shutdown process for it yet.  Release lease for possible others hosts to
//            // pick it up.
//            try
//            {
//                TraceLog.Warning(String.Format("Host '{0}' unable to add PartitionId '{1}' with lease token '{2}' to currently owned partitions.", this.workerName, lease.PartitionId, lease.ConcurrencyToken));
//
//                await this.leaseManager.ReleaseAsync(lease);
//
//                TraceLog.Informational(String.Format("Host '{0}' successfully released lease on PartitionId '{1}' with lease token '{2}'", this.workerName, lease.PartitionId, lease.ConcurrencyToken));
//            }
//            catch (LeaseLostException)
//            {
//                // We have already shutdown the processor so we can ignore any LeaseLost at this point
//                TraceLog.Informational(String.Format("Host '{0}' failed to release lease for PartitionId '{1}' with lease token '{2}' due to conflict.", this.workerName, lease.PartitionId, lease.ConcurrencyToken));
//            }
//            catch (Exception ex)
//            {
//                TraceLog.Exception(ex);
//            }
//        }
    }

    //Java
    Callable<Void> removeLease(T lease, boolean hasOwnership, ChangeFeedObserverCloseReason closeReason){
        if (lease != null && this.currentlyOwnedPartitions != null){
        	
        	String partitionId = lease.getPartitionId();
        	lease = this.currentlyOwnedPartitions.get(partitionId);
        	
        	if(lease != null) {
        		this.currentlyOwnedPartitions.remove(partitionId);
        		
        		TraceLog.informational(String.format("Host '%s' successfully removed PartitionId '%s' with lease token '%s' from currently owned partitions.", this.workerName, lease.getPartitionId(), lease.getConcurrencyToken()));

                try{
                    if (hasOwnership){
                        this.keepRenewingDuringClose.put(lease.getPartitionId(), lease);
                    }

                    TraceLog.informational(String.format("Host '%s' closing event processor for PartitionId '%s' and lease token '%s' with reason '%s'", this.workerName, lease.getPartitionId(), lease.getConcurrencyToken(), closeReason));

                    // Notify the host that we lost partition so shutdown can be triggered on the host
                    this.partitionObserverManager.notifyPartitionReleased(lease, closeReason);

                    TraceLog.informational(String.format("Host '%s' closed event processor for PartitionId '%s' and lease token '%s' with reason '%s'", this.workerName, lease.getPartitionId(), lease.getConcurrencyToken(), closeReason));
                }
                catch (Exception ex){
                    // Eat any exceptions during notification of observers
                    TraceLog.exception(ex);
                }
                finally{
                    if (hasOwnership){
                    	lease = this.keepRenewingDuringClose.get(lease.getPartitionId());
                    	if(lease != null) {
                    		this.keepRenewingDuringClose.remove(lease.getPartitionId());
                    	}   	
                    }
                }

                if (hasOwnership){
                    try
                    {
                        this.leaseManager.release(lease);	//TODO: Equivalent to await?
                        TraceLog.informational(String.format("Host '%s' successfully released lease on PartitionId '%s' with lease token '%s'", this.workerName, lease.getPartitionId(), lease.getConcurrencyToken()));
                    }
                    catch (LeaseLostException ex)
                    {
                        // We have already shutdown the processor so we can ignore any LeaseLost at this point
                        TraceLog.informational(String.format("Host '%s' failed to release lease for PartitionId '%s' with lease token '%s' due to conflict.", this.workerName, lease.getPartitionId(), lease.getConcurrencyToken()));
                    }
                    catch (Exception ex)
                    {
                        TraceLog.exception(ex);
                    }
                }
        		
        	}
        }
        
		return null;
            
    }

}
