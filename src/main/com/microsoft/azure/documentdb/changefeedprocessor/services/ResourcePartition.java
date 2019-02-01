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

import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.changefeedprocessor.ChangeFeedObserverCloseReason;

import java.util.logging.Logger;

public class ResourcePartition {
    private String partitionId;
    private Job resourceJob;
    private Logger logger = Logger.getLogger(ResourcePartition.class.getName());

    public ResourcePartition(String partitionId, Job resourceJob) {
        this.partitionId = partitionId;
        this.resourceJob = resourceJob;
    }

    public void start(String initialData) throws DocumentClientException, InterruptedException {
        logger.info(String.format("Starting ResourceParition: PartitionID: %s - InitialData %S", this.partitionId, initialData));
        resourceJob.start(initialData);
    }

    public void stop() {
        resourceJob.stop(ChangeFeedObserverCloseReason.SHUTDOWN);
    }

    public Job getJob() {
        return resourceJob;
    }
}
