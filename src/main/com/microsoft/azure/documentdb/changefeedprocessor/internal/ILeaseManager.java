/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.microsoft.azure.documentdb.changefeedprocessor.internal;

import com.microsoft.azure.documentdb.DocumentClientException;

/**
 *
 * @author yoterada
 */
/// All of these functions used to be async
public interface ILeaseManager<T extends Lease> {
	boolean leaseStoreExists();

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

	T getLease(String partitionId);

	T acquire(T lease, String owner);

	T renew(T lease) throws LeaseLostException, DocumentClientException;

	boolean release(T lease);

	void delete(T lease);

	void deleteAll();

	boolean isExpired(T lease);
}
