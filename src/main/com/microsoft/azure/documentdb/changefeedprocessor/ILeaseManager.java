/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
//package com.microsoft.azure.documentdb.changefeedprocessor.internal;
package com.microsoft.azure.documentdb.changefeedprocessor;


import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.PartitionKeyRange;

import java.util.Hashtable;
import java.util.concurrent.Callable;

/**
 *
 * @author rogirdh
 */

interface ILeaseManager<T extends Lease> {

	/***
	 * Initialize the LeaseManager properties creating the LeaseCollection if the parameter createLeaseCollection is equal to true.
	 * @param createLeaseCollection - Commented out for now. If uncommented and If true it will create the LeaseCollection, if the collection is not present.
	 * @return Nothing to return
	 * @throws DocumentClientException
	 */
	void initialize(/*boolean createLeaseCollection*/) throws DocumentClientException;

	/***
	 * Checks whether lease store exists
	 * @return return true if exists and false it not
	 * @throws DocumentClientException
	 */
	Callable<Boolean> leaseStoreExists() throws DocumentClientException;

	/***
	 * Checks whether lease store exists and creates if does not exist.
	 * @return true if created, false otherwise.
	 * @throws DocumentClientException
	 */
	Callable<Boolean> createLeaseStoreIfNotExists() throws DocumentClientException;

	/***
	 * List the Leases present in the LeaseCollection
	 * @return Iterable object of the DocumentServiceLease
	 */
	Callable<Iterable<T>> listLeases();

	/***
	 * Checks whether lease exists and creates if does not exist.
	 * @param partitionId: The ID of the partition
	 * @param continuationToken: the continuation token used to create the record.
	 * @return true if created, false otherwise.
	 * @throws DocumentClientException
	 */
	Callable<Boolean> createLeaseIfNotExist(String partitionId, String continuationToken) throws DocumentClientException;

	/***
	 *
	 * @param partitionId
	 * @return
	 * @throws DocumentClientException
	 */
	Callable<T> getLease(String partitionId) throws DocumentClientException;

	/***
	 *
	 * @param lease
	 * @param owner
	 * @return
	 * @throws DocumentClientException
	 * @throws LeaseLostException
	 */
	Callable<T> acquire(T lease, String owner) throws DocumentClientException, LeaseLostException;

	/***
	 *
	 * @param lease
	 * @return
	 * @throws LeaseLostException
	 * @throws DocumentClientException
	 */
	Callable<T> renew(T lease) throws LeaseLostException, DocumentClientException;

	/***
	 *
	 * @param lease
	 * @return
	 * @throws DocumentClientException
	 * @throws LeaseLostException
	 */
	Callable<Boolean> release(T lease) throws DocumentClientException, LeaseLostException;

	/***
	 *
	 * @param lease
	 * @return
	 * @throws DocumentClientException
	 * @throws LeaseLostException
	 */
	Callable<Void> delete(T lease) throws DocumentClientException, LeaseLostException;

	/***
	 *
	 * @return
	 * @throws DocumentClientException
	 * @throws LeaseLostException
	 */
	Callable<Void> deleteAll() throws DocumentClientException, LeaseLostException;

	/***
	 *
	 * @param lease
	 * @return
	 */
	Callable<Boolean> isExpired(T lease);

	void createLeases(Hashtable<String, PartitionKeyRange> ranges);
}
