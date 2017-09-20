/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.microsoft.azure.documentdb.changefeedprocessor.internal;

import com.microsoft.azure.documentdb.changefeedprocessor.*;

/**
 *
 * @author yoterada
 */
public class PartitionManager<T extends Lease> {

//    readonly String workerName;
//    readonly ILeaseManager<T> leaseManager;
//    readonly ChangeFeedHostOptions options;
//    readonly ConcurrentDictionary<String, T> currentlyOwnedPartitions;
//    readonly ConcurrentDictionary<String, T> keepRenewingDuringClose;
//    readonly PartitionObserverManager partitionObserverManager;
//
//    int isStarted;
//    bool shutdownComplete;
//    Task renewTask;
//    Task takerTask;
//    CancellationTokenSource leaseTakerCancellationTokenSource;
//    CancellationTokenSource leaseRenewerCancellationTokenSource;

    public PartitionManager(String workerName, ILeaseManager<T> leaseManager, ChangeFeedHostOptions options)
    {
//        this.workerName = workerName;
//        this.leaseManager = leaseManager;
//        this.options = options;
//
//        this.currentlyOwnedPartitions = new ConcurrentDictionary<String, T>();
//        this.keepRenewingDuringClose = new ConcurrentDictionary<String, T>();
//        this.partitionObserverManager = new PartitionObserverManager(this);
    }

    public void initialize()
    {
//        List<T> leases = new List<T>();
//        List<T> allLeases = new List<T>();
//
//        TraceLog.Verbose(String.Format("Host '{0}' starting renew leases assigned to this host on initialize.", this.workerName));
//
//        foreach (var lease in await this.leaseManager.ListLeases())
//        {
//            allLeases.Add(lease);
//
//            if (String.Compare(lease.Owner, this.workerName, StringComparison.OrdinalIgnoreCase) == 0)
//            {
//                T renewedLease = await this.RenewLeaseAsync(lease);
//                if (renewedLease != null)
//                {
//                    leases.Add(renewedLease);
//                }
//                else
//                {
//                    TraceLog.Informational(String.Format("Host '{0}' unable to renew lease '{1}' on startup.", this.workerName, lease.PartitionId));
//                }
//            }
//        }
//
//        var addLeaseTasks = new List<Task>();
//        foreach (T lease in leases)
//        {
//            TraceLog.Informational(String.Format("Host '{0}' acquired lease for PartitionId '{1}' on startup.", this.workerName, lease.PartitionId));
//            addLeaseTasks.Add(this.AddLeaseAsync(lease));
//        }
//
//        await Task.WhenAll(addLeaseTasks.ToArray());
    }

    public void start()
    {
//        if (Interlocked.CompareExchange(ref this.isStarted, 1, 0) != 0)
//        {
//            throw new InvalidOperationException("Controller has already started");
//        }
//
//        this.shutdownComplete = false;
//        this.leaseTakerCancellationTokenSource = new CancellationTokenSource();
//        this.leaseRenewerCancellationTokenSource = new CancellationTokenSource();
//
//        this.renewTask = await Task.Factory.StartNew(() => this.LeaseRenewer());
//        this.takerTask = await Task.Factory.StartNew(() => this.LeaseTakerAsync());
    }

    public void stop(ChangeFeedObserverCloseReason reason)
    {
//        if (Interlocked.CompareExchange(ref this.isStarted, 0, 1) != 1)
//        {
//            // idempotent
//            return;
//        }
//
//        if (this.takerTask != null)
//        {
//            this.leaseTakerCancellationTokenSource.Cancel();
//            await this.takerTask;
//        }
//
//        await this.ShutdownAsync(reason);
//        this.shutdownComplete = true;
//
//        if (this.renewTask != null)
//        {
//            this.leaseRenewerCancellationTokenSource.Cancel();
//            await this.renewTask;
//        }
//
//        this.leaseTakerCancellationTokenSource = null;
//        this.leaseRenewerCancellationTokenSource = null;
    }

    public IObservableDisposable SubscribeAsync(IPartitionObserver<T> observer)
    {
//        return this.partitionObserverManager.SubscribeAsync(observer);
        return null;
    }

    public void tryReleasePartition(String partitionId, boolean hasOwnership, ChangeFeedObserverCloseReason closeReason)
    {
//        T lease;
//        if (this.currentlyOwnedPartitions.TryGetValue(partitionId, out lease))
//        {
//            await this.RemoveLeaseAsync(lease, hasOwnership, closeReason);
//        }
    }

    void LeaseRenewer()
    {
//        while (this.isStarted == 1 || !this.shutdownComplete)
//        {
//            try
//            {
//                TraceLog.Informational(String.Format("Host '{0}' starting renewal of Leases.", this.workerName));
//
//                ConcurrentBag<T> renewedLeases = new ConcurrentBag<T>();
//                ConcurrentBag<T> failedToRenewLeases = new ConcurrentBag<T>();
//                List<Task> renewTasks = new List<Task>();
//
//                // Renew leases for all currently owned partitions in parallel
//                foreach (T lease in this.currentlyOwnedPartitions.Values)
//                {
//                    renewTasks.Add(this.RenewLeaseAsync(lease).ContinueWith(renewResult =>
//                            {
//                    if (renewResult.Result != null)
//                    {
//                        renewedLeases.Add(renewResult.Result);
//                    }
//                    else
//                    {
//                        // Keep track of all failed attempts to renew so we can trigger shutdown for these partitions
//                        failedToRenewLeases.Add(lease);
//                    }
//                            }));
//                }
//
//                // Renew leases for all partitions currently in shutdown
//                List<T> failedToRenewShutdownLeases = new List<T>();
//                foreach (T shutdownLeases in this.keepRenewingDuringClose.Values)
//                {
//                    renewTasks.Add(this.RenewLeaseAsync(shutdownLeases).ContinueWith(renewResult =>
//                            {
//                    if (renewResult.Result != null)
//                    {
//                        renewedLeases.Add(renewResult.Result);
//                    }
//                    else
//                    {
//                        // Keep track of all failed attempts to renew shutdown leases so we can remove them from further renew attempts
//                        failedToRenewShutdownLeases.Add(shutdownLeases);
//                    }
//                            }));
//                }
//
//                // Wait for all renews to complete
//                await Task.WhenAll(renewTasks.ToArray());
//
//                // Update renewed leases.
//                foreach (T lease in renewedLeases)
//                {
//                    bool updateResult = this.currentlyOwnedPartitions.TryUpdate(lease.PartitionId, lease, lease);
//                    if (!updateResult)
//                    {
//                        TraceLog.Warning(String.Format("Host '{0}' Renewed lease {1} but failed to update it in the map (ignorable).", this.workerName, lease));
//                    }
//                }
//
//                // Trigger shutdown of all partitions we failed to renew leases
//                await failedToRenewLeases.ForEachAsync(
//                    async lease => await this.RemoveLeaseAsync(lease, false, ChangeFeedObserverCloseReason.LeaseLost),
//                    this.options.DegreeOfParallelism);
//
//                // Now remove all failed renewals of shutdown leases from further renewals
//                foreach (T failedToRenewShutdownLease in failedToRenewShutdownLeases)
//                {
//                    T removedLease = null;
//                    this.keepRenewingDuringClose.TryRemove(failedToRenewShutdownLease.PartitionId, out removedLease);
//                }
//
//                await Task.Delay(this.options.LeaseRenewInterval, this.leaseRenewerCancellationTokenSource.Token);
//            }
//            catch (OperationCanceledException)
//            {
//                TraceLog.Informational(String.Format("Host '{0}' Renewer task canceled.", this.workerName));
//            }
//            catch (Exception ex)
//            {
//                TraceLog.Exception(ex);
//            }
//        }
//
//        this.currentlyOwnedPartitions.Clear();
//        this.keepRenewingDuringClose.Clear();
//        TraceLog.Informational(String.Format("Host '{0}' Renewer task completed.", this.workerName));
    }

    void LeaseTaker()
    {
//        while (this.isStarted == 1)
//        {
//            try
//            {
//                TraceLog.Informational(String.Format("Host '{0}' starting to check for available leases.", this.workerName));
//                var availableLeases = await this.TakeLeasesAsync();
//                if (availableLeases.Count > 0) TraceLog.Informational(String.Format("Host '{0}' adding {1} leases...", this.workerName, availableLeases.Count));
//
//                var addLeaseTasks = new List<Task>();
//                foreach (var kvp in availableLeases)
//                {
//                    addLeaseTasks.Add(this.AddLeaseAsync(kvp.Value));
//                }
//
//                await Task.WhenAll(addLeaseTasks.ToArray());
//            }
//            catch (Exception ex)
//            {
//                TraceLog.Exception(ex);
//            }
//
//            try
//            {
//                await Task.Delay(this.options.LeaseAcquireInterval, this.leaseTakerCancellationTokenSource.Token);
//            }
//            catch (OperationCanceledException)
//            {
//                TraceLog.Informational(String.Format("Host '{0}' AcquireLease task canceled.", this.workerName));
//            }
//        }
//
//        TraceLog.Informational(String.Format("Host '{0}' AcquireLease task completed.", this.workerName));
    }

    //IDictionary<String, T>
    Object takeLeases() // it RETURNS IDictionary
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

    void shutdown(ChangeFeedObserverCloseReason reason)
    {
//        var shutdownTasks = this.currentlyOwnedPartitions.Values.Select<T, Task>((lease) =>
//            {
//        return this.RemoveLeaseAsync(lease, true, reason);
//            });
//
//        await Task.WhenAll(shutdownTasks);
    }

    T renewLease(T lease)
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

    void addLease(T lease)
    {
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

    void removeLease(T lease, boolean hasOwnership, ChangeFeedObserverCloseReason closeReason)
    {
//        if (lease != null && this.currentlyOwnedPartitions != null && this.currentlyOwnedPartitions.TryRemove(lease.PartitionId, out lease))
//        {
//            TraceLog.Informational(String.Format("Host '{0}' successfully removed PartitionId '{1}' with lease token '{2}' from currently owned partitions.", this.workerName, lease.PartitionId, lease.ConcurrencyToken));
//
//            try
//            {
//                if (hasOwnership)
//                {
//                    this.keepRenewingDuringClose.TryAdd(lease.PartitionId, lease);
//                }
//
//                TraceLog.Informational(String.Format("Host '{0}' closing event processor for PartitionId '{1}' and lease token '{2}' with reason '{3}'", this.workerName, lease.PartitionId, lease.ConcurrencyToken, closeReason));
//
//                // Notify the host that we lost partition so shutdown can be triggered on the host
//                await this.partitionObserverManager.NotifyPartitionReleasedAsync(lease, closeReason);
//
//                TraceLog.Informational(String.Format("Host '{0}' closed event processor for PartitionId '{1}' and lease token '{2}' with reason '{3}'", this.workerName, lease.PartitionId, lease.ConcurrencyToken, closeReason));
//            }
//            catch (Exception ex)
//            {
//                // Eat any exceptions during notification of observers
//                TraceLog.Exception(ex);
//            }
//            finally
//            {
//                if (hasOwnership)
//                {
//                    this.keepRenewingDuringClose.TryRemove(lease.PartitionId, out lease);
//                }
//            }
//
//            if (hasOwnership)
//            {
//                try
//                {
//                    await this.leaseManager.ReleaseAsync(lease);
//                    TraceLog.Informational(String.Format("Host '{0}' successfully released lease on PartitionId '{1}' with lease token '{2}'", this.workerName, lease.PartitionId, lease.ConcurrencyToken));
//                }
//                catch (LeaseLostException)
//                {
//                    // We have already shutdown the processor so we can ignore any LeaseLost at this point
//                    TraceLog.Informational(String.Format("Host '{0}' failed to release lease for PartitionId '{1}' with lease token '{2}' due to conflict.", this.workerName, lease.PartitionId, lease.ConcurrencyToken));
//                }
//                catch (Exception ex)
//                {
//                    TraceLog.Exception(ex);
//                }
//            }
//        }
    }

}
