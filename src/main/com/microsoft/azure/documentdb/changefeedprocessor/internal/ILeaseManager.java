/*
 * The MIT License (MIT)
 * Copyright (c) 2018 Microsoft Corporation
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

import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.PartitionKeyRange;

import java.util.Hashtable;
import java.util.List;
import java.util.concurrent.Callable;

public interface ILeaseManager<T extends Lease> {

	/***
	 * Initialize the LeaseManager properties creating the LeaseCollection if the parameter createLeaseCollection is equal to true.
	 * @param createLeaseCollection - If true it will create the LeaseCollection, if the collection is not present.
	 * @throws DocumentClientException thrown
	 */
	void initialize(boolean createLeaseCollection) throws DocumentClientException;

	/***
	 * Checks whether lease store exists
	 * @return return true if exists and false it not
	 * @throws DocumentClientException thrown
	 */
	Callable<Boolean> leaseStoreExists() throws DocumentClientException;

	/***
	 * Checks whether lease store exists and creates if does not exist.
	 * @return true if created, false otherwise.
	 * @throws DocumentClientException thrown
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
	 * @throws DocumentClientException thrown
	 */
	Callable<Boolean> createLeaseIfNotExist(String partitionId, String continuationToken) throws DocumentClientException;

	/***
	 *
	 * @param partitionId the partition ID
	 * @return the Callable object
	 * @throws DocumentClientException thrown
	 */
	Callable<T> getLease(String partitionId) throws DocumentClientException;

	/***
	 *
	 * @param lease the lease
	 * @param owner the owner
	 * @return the Callable object
	 * @throws DocumentClientException thrown
	 * @throws LeaseLostException thrown
	 */
	Callable<T> acquire(T lease, String owner) throws DocumentClientException, LeaseLostException;

	/***
	 *
	 * @param lease the lease
	 * @return the Callable object
	 * @throws DocumentClientException thrown
	 * @throws LeaseLostException thrown
	 */
	Callable<T> renew(T lease) throws LeaseLostException, DocumentClientException;

	/***
	 *
	 * @param lease the lease
	 * @return the Callable object
	 * @throws DocumentClientException thrown
	 * @throws LeaseLostException thrown
	 */
	Callable<Boolean> release(T lease) throws DocumentClientException, LeaseLostException;

	/***
	 *
	 * @param lease the lease
	 * @return nothing
	 * @throws DocumentClientException thrown
	 * @throws LeaseLostException thrown
	 */
	Callable<Void> delete(T lease) throws DocumentClientException, LeaseLostException;

	/***
	 *
	 * @return nothing
	 * @throws DocumentClientException throws
	 * @throws LeaseLostException throws
	 */
	Callable<Void> deleteAll() throws DocumentClientException, LeaseLostException;

	/***
	 *
	 * @param lease the lease
	 * @return Callable object
	 */
	Callable<Boolean> isExpired(T lease);

	void createLeases(Hashtable<String, PartitionKeyRange> ranges);

}
