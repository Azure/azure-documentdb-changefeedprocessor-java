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

import java.time.Instant;

public class ChangeFeedHostOptions {

    static Instant DefaultRenewInterval =  Instant.ofEpochMilli(0).plusSeconds(17);
    static Instant DefaultAcquireInterval = Instant.ofEpochMilli(0).plusSeconds(13);
    static Instant DefaultExpirationInterval = Instant.ofEpochMilli(0).plusSeconds(60);
    static Instant DefaultFeedPollDelay = Instant.ofEpochMilli(0).plusSeconds(5);

    public ChangeFeedHostOptions(){
        leaseRenewInterval = DefaultRenewInterval;
        leaseAcquireInterval = DefaultAcquireInterval;
        leaseExpirationInterval = DefaultExpirationInterval;
        feedPollDelay = DefaultFeedPollDelay;
    }

    private Instant leaseRenewInterval;
    private Instant leaseAcquireInterval;
    private Instant leaseExpirationInterval;
    private Instant feedPollDelay;
    private CheckpointFrequency checkpointFrequency;
    private String leasePrefix;
    int minPartitionCount;
    int maxPartitionCount;
    boolean discardExistingLeases;
    int degreeOfParallelism;

    int getDegreeOfParallelism(){
        return this.maxPartitionCount > 0 ? this.maxPartitionCount : 25;
    }

    public Instant getLeaseRenewInterval() {
        return leaseRenewInterval;
    }

    public void setLeaseRenewInterval(Instant _leaseRenewInterval) {
        this.leaseRenewInterval = _leaseRenewInterval;
    }

    public Instant getLeaseAcquireInterval() {
        return leaseAcquireInterval;
    }

    public void setLeaseAcquireInterval(Instant _leaseAcquireInterval) {
        this.leaseAcquireInterval = _leaseAcquireInterval;
    }

    public Instant getLeaseExpirationInterval() {
        return leaseExpirationInterval;
    }

    public void setLeaseExpirationInterval(Instant _leaseExpirationInterval) {
        this.leaseExpirationInterval = _leaseExpirationInterval;
    }

    public Instant getFeedPollDelay() {
        return feedPollDelay;
    }

    public void setFeedPollDelay(Instant _feedPollDelay) {
        this.feedPollDelay = _feedPollDelay;
    }

    public CheckpointFrequency getCheckpointFrequency() {
        return checkpointFrequency;
    }

    public void setCheckpointFrequency(CheckpointFrequency _checkpointFrequency) {
        this.checkpointFrequency = _checkpointFrequency;
    }

    public String getLeasePrefix() {
        return leasePrefix;
    }

    public void setLeasePrefix(String _leasePrefix) {
        this.leasePrefix = _leasePrefix;
    }

    public int getMinPartitionCount() {
        return minPartitionCount;
    }

    void setMinPartitionCount(int _minPartitionCount) {
        this.minPartitionCount = _minPartitionCount;
    }

    int getMaxPartitionCount() {
        return maxPartitionCount;
    }

    void setMaxPartitionCount(int _maxPartitionCount) {
        this.maxPartitionCount = _maxPartitionCount;
    }

    boolean isDiscardExistingLeases() {
        return discardExistingLeases;
    }

    void setDiscardExistingLeases(boolean _discardExistingLeases) {
        this.discardExistingLeases = _discardExistingLeases;
    }

    boolean getDiscardExistingLeases() {
        return discardExistingLeases;
    }
}