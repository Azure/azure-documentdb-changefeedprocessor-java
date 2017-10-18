/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.microsoft.azure.documentdb.changefeedprocessor.internal;


import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import com.microsoft.azure.documentdb.DocumentClientException;

/**
 *
 * @author rogirdh
 */
/// TODO: All of these functions used to be async
public interface ILeaseManager<T extends Lease> {
	Callable<Boolean> leaseStoreExists() throws DocumentClientException;

	/// <summary>
	/// Checks whether lease store exists and creates if does not exist.
	/// </summary>
	/// <returns>true if created, false otherwise.</returns>
	Callable<Boolean> createLeaseStoreIfNotExists() throws DocumentClientException;

	Callable<Iterable<T>> listLeases();

	/// <summary>
	/// Checks whether lease exists and creates if does not exist.
	/// </summary>
	/// <returns>true if created, false otherwise.</returns>
	Callable<Boolean> createLeaseIfNotExist(String partitionId, String continuationToken) throws DocumentClientException;

	Callable<T> getLease(String partitionId) throws DocumentClientException;

	Callable<T> acquire(T lease, String owner) throws DocumentClientException, LeaseLostException;

	Callable<T> renew(T lease) throws LeaseLostException, DocumentClientException;

	Callable<Boolean> release(T lease) throws DocumentClientException, LeaseLostException;

	Runnable delete(T lease) throws DocumentClientException, LeaseLostException;

	Runnable deleteAll() throws DocumentClientException, LeaseLostException;

	Callable<Boolean> isExpired(T lease);
}
