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

import com.microsoft.azure.documentdb.ChangeFeedOptions;
import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.FeedResponse;
import com.microsoft.azure.documentdb.changefeedprocessor.ChangeFeedObserverCloseReason;
import com.microsoft.azure.documentdb.changefeedprocessor.ChangeFeedObserverContext;
import com.microsoft.azure.documentdb.changefeedprocessor.IChangeFeedObserver;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.ChangeFeedThreadFactory;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.StatusCode;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.SubStatusCode;

import java.util.List;
import java.util.concurrent.*;
import java.util.logging.Logger;

public class ChangeFeedJob implements Job {

    private final DocumentServices client;
    private final CheckpointServices checkpointServices;

    private final String partitionId;
    private final IChangeFeedObserver observer;
    private ChangeFeedOptions feedOptions;
    private int pageSize;
    private final int DEFAULT_PAGE_SIZE = 100;
    private final int DEFAULT_THREAD_WAIT = 1000;
    private ExecutorService exec;
    private final int CPUs = Runtime.getRuntime().availableProcessors();
    private final int NumThreadsPerCpu = 5;
    private static Logger logger = Logger.getLogger(ChangeFeedJob.class.getName());

    /**
     * Gets the client associated with the resource.
     *
     * @return the client associated with the resource.
     */
    public DocumentServices getClient() {
        return this.client;
    }

    /**
     * Gets the checkpoint services associated with the resource.
     *
     * @return the checkpoint services associated with the resource.
     */
    public CheckpointServices getCheckpointServices() {
        return this.checkpointServices;
    }

    /***
     *
     * @param partitionId
     * @param client
     * @param checkpointServices
     * @param observer
     * @param pageSize
     */
    public ChangeFeedJob(String partitionId,
                          DocumentServices client,
                          CheckpointServices checkpointServices,
                          IChangeFeedObserver observer,
                          int pageSize) {
        this.client = client;
        this.checkpointServices = checkpointServices;
        this.partitionId = partitionId;
        this.observer = observer;
        this.pageSize = pageSize;

        exec = CreateExecutorService(NumThreadsPerCpu, "partition_" + partitionId);
    }

    /**
     *
     * @param partitionId
     * @param client
     * @param checkpointServices
     * @param observer
     */
    public ChangeFeedJob(String partitionId,
                          DocumentServices client,
                          CheckpointServices checkpointServices,
                          IChangeFeedObserver observer) {
        this.client = client;
        this.checkpointServices = checkpointServices;
        this.partitionId = partitionId;
        this.observer = observer;
        this.pageSize = DEFAULT_PAGE_SIZE;

        exec = CreateExecutorService(NumThreadsPerCpu, "partition_" + partitionId);
    }


    private ExecutorService CreateExecutorService(int numThreadPerCPU, String threadSuffixName ){

        logger.info(String.format("Creating ExecutorService CPUs: %d, numThreadPerCPU: %d, threadSuffixName: %s",CPUs, numThreadPerCPU, threadSuffixName));
        if (numThreadPerCPU <= 0) throw new IllegalArgumentException("The parameter numThreadPerCPU must be greater them 0");
        if (threadSuffixName == null || threadSuffixName.isEmpty()) throw new IllegalArgumentException("The parameter threadSuffixName is null or empty");

        ChangeFeedThreadFactory threadFactory = new ChangeFeedThreadFactory(threadSuffixName);
        ExecutorService exec = Executors.newFixedThreadPool(numThreadPerCPU * CPUs, threadFactory);

        return exec;
    }

    @Override
    public void start(String initialData) throws DocumentClientException, InterruptedException {
        logger.info(String.format("Starting ChangeFeedJob "));
        if (!exec.isShutdown() && !exec.isTerminated()) {

            exec.execute(() -> {
                try {
                    QueryChangeFeed(initialData);
                } catch (DocumentClientException e) {
                    logger.warning(e.getMessage());
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    logger.warning(e.getMessage());
                    e.printStackTrace();
                }
            });
        }
    }

    private void QueryChangeFeed(String initialData) throws DocumentClientException, InterruptedException{
        logger.info(String.format("PartitionID: %s - QueryChangeFeed - Initiate", partitionId));
        ChangeFeedObserverContext context = new ChangeFeedObserverContext();
        context.setPartitionKeyRangeId(partitionId);
        FeedResponse<Document> query = null;

        boolean HasMoreResults = false;

        while(!(exec.isTerminated() || exec.isShutdown())) {
            do {
                try {
                    logger.info(String.format("client.createDocumentChangeFeedQuery(%s, %s, %d)",partitionId, checkpointServices.getCheckpointData(partitionId), this.pageSize));
                    query = client.createDocumentChangeFeedQuery(partitionId, checkpointServices.getCheckpointData(partitionId), this.pageSize);
                    if (query != null) {
                        logger.info(String.format("Query is not null query.getActivityId: %s ", query.getActivityId()));
                        context.setFeedResponse(query);
                        List<Document> docs = query.getQueryIterable().fetchNextBlock();
                        HasMoreResults = query.getQueryIterator().hasNext();
                        if (docs != null) {
                            logger.info(String.format("Docs Loaded #%d - HasMoreResults: %s",docs.size(), HasMoreResults));
                            observer.processChanges(context, docs);
                            this.checkpoint(query.getResponseContinuation());
                        }else{
                            logger.info(String.format("Docs is null & HasMoreResults = %s", HasMoreResults));
                        }
                    }
                } catch (DocumentClientException dce) {
                    int subStatusCode = getSubStatusCode(dce);
                    if ((dce.getStatusCode() == StatusCode.NOTFOUND.Value() &&
                            SubStatusCode.ReadSessionNotAvailable.Value() != subStatusCode) ||
                            dce.getStatusCode() == StatusCode.GONE.Value()){
                        this.stop(ChangeFeedObserverCloseReason.RESOURCE_GONE);
                        observer.close(context,ChangeFeedObserverCloseReason.RESOURCE_GONE );
                    }
                    else if (SubStatusCode.Splitting.Value() == subStatusCode)
                    {
                        logger.warning(String.format("Partition %s is splitting. Will retry to read changes until split finishes. %s", context.getPartitionKeyRangeId(), dce.getMessage()));
                    }
                    else if (dce.getStatusCode() == StatusCode.TOO_MANY_REQUESTS.Value())
                    {
                        //Wait the thread a little more to cool down the hit.
                        try {
                            exec.awaitTermination(this.DEFAULT_THREAD_WAIT, TimeUnit.MILLISECONDS);
                            logger.info(String.format("Too many requests during change feed for Partition: %s - Waiting for %d milliseconds before perform another query.", this.partitionId, this.DEFAULT_THREAD_WAIT));
                        }catch (InterruptedException e){
                            logger.warning(String.format("Too Many requests InterruptedException trying to wait the thread: %s", e.getMessage()));
                        }
                    }
                    else
                    {
                        throw dce;
                    }
                }catch (Exception ex){
                    logger.severe(String.format("Other exception not handled happened: %s ",ex.getMessage()));
                    ex.printStackTrace();
                }

            }while (HasMoreResults && !(exec.isTerminated() || exec.isShutdown()) );

            if (!(exec.isTerminated() || exec.isShutdown()))
            {
                try {
                    exec.awaitTermination(this.DEFAULT_THREAD_WAIT, TimeUnit.MILLISECONDS);
                    logger.info(String.format("There are no changes for Partition: %s - Waiting for %d milliseconds before perform another query.", this.partitionId, this.DEFAULT_THREAD_WAIT));
                }catch (InterruptedException e){
                    logger.warning(String.format(" InterruptedException trying to wait the thread: %s", e.getMessage()));
                }
            }

        }
    }

    void checkpoint(String data) throws DocumentClientException {
        logger.info(String.format("Chekpoint - PartitionID: %s, InitialData %s",this.partitionId, data));
        String initialData = "";

        if (data != null)
            initialData = data;

        checkpointServices.setCheckpointData(partitionId, initialData);
    }

    @Override
    public void stop(ChangeFeedObserverCloseReason CloseReason) {

        switch (CloseReason){
            case SHUTDOWN:
            case RESOURCE_GONE:
                logger.warning(String.format("CloseReason %s Shutting down executor", CloseReason));
                exec.shutdown();
                break;
            default:
                logger.warning(String.format("CloseReason %s Shutting down executor NOW", CloseReason));
                exec.shutdownNow();
                break;
        }

    }

    private int getSubStatusCode(DocumentClientException exception)
    {
        String SubStatusHeaderName = "x-ms-substatus";
        String valueSubStatus = exception.getResponseHeaders().get(SubStatusHeaderName);
        if (valueSubStatus != null && !valueSubStatus.isEmpty())
        {
            int subStatusCode = 0;
            try {
                return Integer.parseInt(valueSubStatus);
            }catch (Exception e){
                logger.severe(String.format("Not able to parse the error code %s to int", valueSubStatus));
            }
        }

        return -1;
    }

}
