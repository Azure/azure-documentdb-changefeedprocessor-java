/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.microsoft.azure.documentdb.changefeedprocessor.internal;

/**
 *
 * @author fcatae
 */
/// <summary>
/// Provides methods for running checkpoint asynchronously. Extensibility is provided to specify host-specific storage for storing the offset.
/// </summary>
//TODO: Need to convert this to Async
public interface ICheckpointManager {

    /// <summary>Stores the offset of a particular partition in the host-specific store.</summary>
    /// <param name="lease">Partition information against which to perform a checkpoint.</param>
    /// <param name="offset">Current position in the stream.</param>
    /// <param name="sequenceNumber">The sequence number of the partition.</param>
    /// <returns>Returns <see cref="System.Threading.Tasks.Task" />.</returns>
    Lease checkpoint(Lease lease, String offset, long sequenceNumber);
    
 //   Lease checkpointAsync(Lease lease, String offset, long sequenceNumber);
}
