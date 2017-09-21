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
        _leaseRenewInterval = DefaultRenewInterval;
        _leaseAcquireInterval = DefaultAcquireInterval;
        _leaseExpirationInterval = DefaultExpirationInterval;
        _feedPollDelay = DefaultFeedPollDelay;
    }

    private Instant _leaseRenewInterval;
    private Instant _leaseAcquireInterval;
    private Instant _leaseExpirationInterval;
    private Instant _feedPollDelay;
    private CheckpointFrequency _checkpointFrequency;
    private String _leasePrefix;
    int _minPartitionCount;
    int _maxPartitionCount;
    boolean _discardExistingLeases;
    int DegreeOfParallelism;
    private int pageSize;

    int getDegreeOfParallelism(){
        return this._maxPartitionCount > 0 ? this._maxPartitionCount : 25;
    }

    public Instant getLeaseRenewInterval() {
        return _leaseRenewInterval;
    }

    public void setLeaseRenewInterval(Instant _leaseRenewInterval) {
        this._leaseRenewInterval = _leaseRenewInterval;
    }

    public Instant getLeaseAcquireInterval() {
        return _leaseAcquireInterval;
    }

    public void setLeaseAcquireInterval(Instant _leaseAcquireInterval) {
        this._leaseAcquireInterval = _leaseAcquireInterval;
    }

    public Instant getLeaseExpirationInterval() {
        return _leaseExpirationInterval;
    }

    public void setLeaseExpirationInterval(Instant _leaseExpirationInterval) {
        this._leaseExpirationInterval = _leaseExpirationInterval;
    }

    public Instant getFeedPollDelay() {
        return _feedPollDelay;
    }

    public void setFeedPollDelay(Instant _feedPollDelay) {
        this._feedPollDelay = _feedPollDelay;
    }

    public CheckpointFrequency getCheckpointFrequency() {
        return _checkpointFrequency;
    }

    public void setCheckpointFrequency(CheckpointFrequency _checkpointFrequency) {
        this._checkpointFrequency = _checkpointFrequency;
    }

    public String getLeasePrefix() {
        return _leasePrefix;
    }

    public void setLeasePrefix(String _leasePrefix) {
        this._leasePrefix = _leasePrefix;
    }

    public int getMinPartitionCount() {
        return _minPartitionCount;
    }

    void setMinPartitionCount(int _minPartitionCount) {
        this._minPartitionCount = _minPartitionCount;
    }

    int getMaxPartitionCount() {
        return _maxPartitionCount;
    }

    void setMaxPartitionCount(int _maxPartitionCount) {
        this._maxPartitionCount = _maxPartitionCount;
    }

    boolean isDiscardExistingLeases() {
        return _discardExistingLeases;
    }

    void setDiscardExistingLeases(boolean _discardExistingLeases) {
        this._discardExistingLeases = _discardExistingLeases;
    }

    public int getPageSize(){return this.pageSize;}
    public void setPageSize(int pageSize){this.pageSize = pageSize;}
}