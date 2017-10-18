/*
 *
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
 *  
 */

package com.microsoft.azure.documentdb.changefeedprocessor;

import java.time.Duration;

public class ChangeFeedHostOptions {

    static Duration DefaultRenewInterval =  Duration.ofMillis(0).plusSeconds(17);
    static Duration DefaultAcquireInterval = Duration.ofMillis(0).plusSeconds(13);
    static Duration DefaultExpirationInterval = Duration.ofMillis(0).plusSeconds(60);
    static Duration DefaultFeedPollDelay = Duration.ofMillis(0).plusSeconds(5);

    public ChangeFeedHostOptions(){
        leaseRenewInterval = DefaultRenewInterval;
        leaseAcquireInterval = DefaultAcquireInterval;
        leaseExpirationInterval = DefaultExpirationInterval;
        feedPollDelay = DefaultFeedPollDelay;
    }

    private Duration leaseRenewInterval;
    private Duration leaseAcquireInterval;
    private Duration leaseExpirationInterval;
    private Duration feedPollDelay;
    private CheckpointFrequency checkpointFrequency;
    private String leasePrefix;
    int minPartitionCount;
    int maxPartitionCount;
    boolean discardExistingLeases;
    int degreeOfParallelism;

    int getDegreeOfParallelism(){
        return this.maxPartitionCount > 0 ? this.maxPartitionCount : 25;
    }

    public Duration getLeaseRenewInterval() {
        return leaseRenewInterval;
    }

    public void setLeaseRenewInterval(Duration leaseRenewInterval) {
        this.leaseRenewInterval = leaseRenewInterval;
    }

    public Duration getLeaseAcquireInterval() {
        return leaseAcquireInterval;
    }

    public void setLeaseAcquireInterval(Duration leaseAcquireInterval) {
        this.leaseAcquireInterval = leaseAcquireInterval;
    }

    public Duration getLeaseExpirationInterval() {
        return leaseExpirationInterval;
    }

    public void setLeaseExpirationInterval(Duration leaseExpirationInterval) {
        this.leaseExpirationInterval = leaseExpirationInterval;
    }

    public Duration getFeedPollDelay() {
        return feedPollDelay;
    }

    public void setFeedPollDelay(Duration feedPollDelay) {
        this.feedPollDelay = feedPollDelay;
    }

    public CheckpointFrequency getCheckpointFrequency() {
        return checkpointFrequency;
    }

    public void setCheckpointFrequency(CheckpointFrequency checkpointFrequency) {
        this.checkpointFrequency = checkpointFrequency;
    }

    public String getLeasePrefix() {
        return leasePrefix;
    }

    public void setLeasePrefix(String leasePrefix) {
        this.leasePrefix = leasePrefix;
    }

    public int getMinPartitionCount() {
        return minPartitionCount;
    }

    void setMinPartitionCount(int minPartitionCount) {
        this.minPartitionCount = minPartitionCount;
    }

    int getMaxPartitionCount() {
        return maxPartitionCount;
    }

    void setMaxPartitionCount(int maxPartitionCount) {
        this.maxPartitionCount = maxPartitionCount;
    }

    boolean isDiscardExistingLeases() {
        return discardExistingLeases;
    }

    void setDiscardExistingLeases(boolean discardExistingLeases) {
        this.discardExistingLeases = discardExistingLeases;
    }

    boolean getDiscardExistingLeases() {
        return discardExistingLeases;
    }
}