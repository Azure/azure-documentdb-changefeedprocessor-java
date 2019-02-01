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
import com.microsoft.azure.documentdb.changefeedprocessor.IChangeFeedObserverFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

public class ResourcePartitionServices {
    private CheckpointServices checkpointSvcs;
    private ConcurrentHashMap<String, ResourcePartition> resourcePartitions;
    private DocumentServices client;
    private IChangeFeedObserverFactory factory;
    private int pageSize;
    private Logger logger = Logger.getLogger(ResourcePartitionServices.class.getName());

    public ResourcePartitionServices(DocumentServices client, CheckpointServices checkpointSvcs, IChangeFeedObserverFactory factory, int pageSize) {

        resourcePartitions = new ConcurrentHashMap<>();
        this.client = client;
        this.checkpointSvcs = checkpointSvcs;
        this.factory = factory;
        this.pageSize = pageSize;
    }

    public ResourcePartition create(String partitionId) {
        logger.info(String.format("Creating job for Partition %s", partitionId));
        Job job = null;
        try {
            job = new ChangeFeedJob(partitionId, client, checkpointSvcs, factory.createObserver(), pageSize);
        } catch (IllegalAccessException e) {
            logger.severe(e.getMessage());
            e.printStackTrace();
        } catch (InstantiationException e) {
            logger.severe(e.getMessage());
            e.printStackTrace();
        }
        ResourcePartition resourcePartition = new ResourcePartition(partitionId, job);

        logger.info("Adding partition to the resourcePartitions dictionary");
        resourcePartitions.put(partitionId, resourcePartition);

        return resourcePartition;
    }

    private ResourcePartition get(String partitionId) {
        return resourcePartitions.get(partitionId);
    }

    public void start(String partitionId) throws DocumentClientException, InterruptedException {
        ResourcePartition resourcePartition = this.get(partitionId);
        String initialData = checkpointSvcs.getCheckpointData(partitionId);
        logger.info(String.format("Starting partition %s - Checkpoint %s ",partitionId, initialData));
        resourcePartition.start(initialData);
    }

    public void stop(String partitionId) {
        // TODO: improve it
        ResourcePartition resourcePartition = this.get(partitionId);
        resourcePartition.stop();
    }
}