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
 * @author yoterada
 */
/// TODO: All of these functions used to be async
public interface ILeaseManager<T extends Lease> {
	boolean leaseStoreExists() throws DocumentClientException;

	/// <summary>
	/// Checks whether lease store exists and creates if does not exist.
	/// </summary>
	/// <returns>true if created, false otherwise.</returns>
	boolean createLeaseStoreIfNotExists() throws DocumentClientException;

	Iterable<T> listLeases();

	/// <summary>
	/// Checks whether lease exists and creates if does not exist.
	/// </summary>
	/// <returns>true if created, false otherwise.</returns>
	boolean createLeaseIfNotExist(String partitionId, String continuationToken) throws DocumentClientException;

	T getLease(String partitionId) throws DocumentClientException;

	Future<T> acquire(T lease, String owner) throws DocumentClientException, LeaseLostException;

	Future<T> renew(T lease) throws LeaseLostException, DocumentClientException;

	boolean release(T lease) throws DocumentClientException, LeaseLostException;

	void delete(T lease) throws DocumentClientException, LeaseLostException;

	void deleteAll() throws DocumentClientException, LeaseLostException;

	boolean isExpired(T lease);
}
