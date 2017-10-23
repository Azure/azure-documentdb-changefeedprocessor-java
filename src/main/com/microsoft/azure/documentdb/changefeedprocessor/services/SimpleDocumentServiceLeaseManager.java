/**
 * The MIT License (MIT)
 * Copyright (c) 2016 Microsoft Corporation
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
package com.microsoft.azure.documentdb.changefeedprocessor.services;

import com.microsoft.azure.documentdb.*;
import com.microsoft.azure.documentdb.changefeedprocessor.DocumentCollectionInfo;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.*;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.documentleasestore.DocumentServiceLease;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.documentleasestore.DocumentServiceLeaseManager;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.documentleasestore.LeaseState;
import org.apache.http.HttpStatus;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 *
 * @author yoterada
 */
public class SimpleDocumentServiceLeaseManager implements ILeaseManager<DocumentServiceLease>, ICheckpointManager {
    private final static String DATE_HEADER_NAME = "Date";
    private final static String CONTAINER_SEPARATOR = ".";
    private final static String PARTITION_PREFIX = ".";
    private final static String CONTAINER_NAME_SUFFIX = "info";
    private final static int RETRY_COUNT_ON_CONFLICT = 5;
    private String containerNamePrefix;
    private DocumentCollectionInfo leaseStoreCollectionInfo;
    private Duration leaseIntervalAllowance = Duration.ofMillis(25);
    private Instant leaseInterval;
    private Instant renewInterval;

    private String leaseStoreCollectionLink;
    private Duration serverToLocalTimeDelta;

    DocumentClient client;

    @FunctionalInterface
    private interface LeaseConflictResolver {
        DocumentServiceLease run(DocumentServiceLease serverLease);
    }

    public SimpleDocumentServiceLeaseManager (DocumentCollectionInfo leaseStoreCollectionInfo, String storeNamePrefix, Instant leaseInterval, Instant renewInterval) {
        this.leaseStoreCollectionInfo = leaseStoreCollectionInfo;
        this.containerNamePrefix = storeNamePrefix;
        this.leaseInterval = leaseInterval;
        this.renewInterval = renewInterval;
        this.client = new DocumentClient(leaseStoreCollectionInfo.getUri().toString(), leaseStoreCollectionInfo.getMasterKey(), leaseStoreCollectionInfo.getConnectionPolicy(), ConsistencyLevel.Session);
    }

    public void dispose() {
    }

    public void initialize() throws DocumentClientException {
    }

    @Override
    public boolean leaseStoreExists() throws DocumentClientException {
        return true;
    }

    @Override
    public boolean createLeaseStoreIfNotExists() throws DocumentClientException {
        return true;
    }

    @Override
    public Iterable<DocumentServiceLease> listLeases() {//    public Task<IEnumerable<DocumentServiceLease>> ListLeases()
        return null;
    }

    @Override
    public boolean createLeaseIfNotExist(String partitionId, String continuationToken) throws DocumentClientException {
        return true;
    }

    @Override
    public DocumentServiceLease getLease(String partitionId) throws DocumentClientException {//    public async Task<DocumentServiceLease> GetLeaseAsync(string partitionId)
        return null;
    }

    @Override
    public DocumentServiceLease acquire(DocumentServiceLease lease, String owner) throws DocumentClientException {
        return null;
    }

    @Override
    public DocumentServiceLease renew(DocumentServiceLease lease) throws LeaseLostException, DocumentClientException {
        return null;
    }

    @Override
    public boolean release(DocumentServiceLease lease) throws DocumentClientException, LeaseLostException {
        return true;
    }

    @Override
    public void delete(DocumentServiceLease lease) throws DocumentClientException, LeaseLostException {
    }

    @Override
    public void deleteAll() throws DocumentClientException, LeaseLostException {
    }

    @Override
    public boolean isExpired(DocumentServiceLease lease) {
        return true;
    }

    @Override
    public Lease checkpoint(Lease lease, String continuationToken, long sequenceNumber) {
        return null;
    }

}

