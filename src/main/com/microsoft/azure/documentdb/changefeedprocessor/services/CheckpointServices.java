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
package com.microsoft.azure.documentdb.changefeedprocessor.services;

import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.changefeedprocessor.CheckpointFrequency;
import com.microsoft.azure.documentdb.changefeedprocessor.CheckpointStats;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.ICheckpointManager;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.ILeaseManager;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.documentleasestore.DocumentServiceLease;

import java.time.Instant;
import java.util.logging.Logger;

public class CheckpointServices {

    private CheckpointFrequency checkpointOptions;
    private ICheckpointManager checkpointManager;
    private ILeaseManager leaseManager;
    private Logger logger = Logger.getLogger(CheckpointServices.class.getName());

    public CheckpointServices(ILeaseManager leaseManager, CheckpointFrequency checkpointOptions){
        this.leaseManager = leaseManager;
        this.checkpointManager = (ICheckpointManager)leaseManager;
        this.checkpointOptions = checkpointOptions;
    }

    public String getCheckpointData(String partitionId) throws DocumentClientException {
        String data = getCheckpoint(partitionId);
        if (data == null)
            data = "";
        logger.info(String.format("Retrieving Checkpoint partitionId: %s, data: %s",partitionId,data));

        return data;
    }

    public void setCheckpointData(String partitionId, String data) throws DocumentClientException {
        logger.info(String.format("Saving Checkpoint partitionId: %s, data: %s",partitionId,data));
        DocumentServiceLease lease = null;
        try {
            lease = (DocumentServiceLease)leaseManager.getLease(partitionId).call();
        } catch (Exception e) {
            e.printStackTrace();
        }
        String continuation = data;
        checkpoint(lease, continuation);
    }

    String getCheckpoint(String partitionId) throws DocumentClientException {
        DocumentServiceLease lease = null;
        try {
            lease = (DocumentServiceLease) this.leaseManager.getLease(partitionId).call();
        } catch (Exception e) {
            logger.severe(String.format("Error!! %s", e.getMessage()));
            e.printStackTrace();
        }
        return lease.getContinuationToken();
    }

    public void checkpoint(DocumentServiceLease lease, String continuation) {
        if ((lease == null)) throw new AssertionError();
        if ((continuation == null || continuation == "")) throw new AssertionError();

        DocumentServiceLease result = null;
        try
        {
            result = (DocumentServiceLease) this.checkpointManager.checkpoint(lease, continuation, lease.getSequenceNumber() + 1);

            if(result.getConcurrencyToken() == continuation )
                logger.info(String.format("Checkpoint: partition %s, continuation token '%s' was not updated!", lease.getPartitionId(), continuation));
            else
                logger.info(String.format("Checkpoint: partition %s, new continuation '%s'", lease.getPartitionId(), continuation));
        }
        catch (Exception ex)
        {
            logger.severe(String.format("Partition %s: failed to checkpoint due to unexpected error: $s", lease.getPartitionId(), ex.getMessage()));
            throw ex;
        }
    }
    public boolean isCheckpointNeeded(DocumentServiceLease lease, CheckpointStats checkpointStats)
    {
        CheckpointFrequency options = this.checkpointOptions;

        assert (lease != null);
        assert (checkpointStats != null);

        if (checkpointStats.getProcessedDocCount() == 0) {
            return false;
        }

        boolean isCheckpointNeeded = true;

        boolean hasProcessedDocumentCount = options.getProcessedDocumentCount().isPresent();
        boolean hasTimeInterval = options.getTimeInterval().isPresent();

        if (options != null && (hasProcessedDocumentCount || hasTimeInterval)) {
            // Note: if either condition is satisfied, we checkpoint.
            isCheckpointNeeded = false;

            if (hasProcessedDocumentCount) {
                isCheckpointNeeded = (checkpointStats.getProcessedDocCount() >= options.getProcessedDocumentCount().get());
            }

            if (hasTimeInterval) {
                isCheckpointNeeded = isCheckpointNeeded ||
                        (Instant.now().getEpochSecond() - checkpointStats.getLastCheckpointTime().getEpochSecond()) >= options.getTimeInterval().get();
            }
        }

        return isCheckpointNeeded;
    }
}