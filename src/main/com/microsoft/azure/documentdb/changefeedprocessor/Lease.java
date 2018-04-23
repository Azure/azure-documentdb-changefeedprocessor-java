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
//package com.microsoft.azure.documentdb.changefeedprocessor.internal;
package com.microsoft.azure.documentdb.changefeedprocessor;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;



public abstract class Lease {

    @JsonProperty("PartitionId")
    private String partitionId;

    @JsonProperty("Owner")
    private String owner;

    @JsonProperty("ContinuationToken")
    private String continuationToken;

    @JsonProperty("SequenceNumber")
    private long sequenceNumber;

    @JsonIgnore
    private String concurrencyToken;

    public String getPartitionId(){
        return partitionId;
    }

    public void setPartitionId(String _partitionId){
        this.partitionId = _partitionId;
    }

    public String getOwner(){
        return owner;
    }

    public void setOwner(String _owner){
        this.owner = _owner;
    }

    public String getContinuationToken(){
        return continuationToken;
    }

    public void setContinuationToken(String _continuationToken){
        this.continuationToken = _continuationToken;
    }

    public long getSequenceNumber(){
        return sequenceNumber;
    }

    public void setSequenceNumber(long _sequenceNumber){
        this.sequenceNumber = _sequenceNumber;
    }

    public String getConcurrencyToken() {
        return concurrencyToken;
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof Lease && obj.equals(this)){
            return true;
        }

        Lease lease = (Lease) obj;
        if(lease == null) {
            return false;
        }

        return this.partitionId.equals(lease.partitionId);
    }

    @Override
    public int hashCode(){
        return this.partitionId.hashCode();
    }

    public Lease() {
        partitionId = null;
        owner = "";
    }

    public Lease(Lease source) {
        this.partitionId = source.getPartitionId();
        this.owner = source.getOwner();
        this.continuationToken = source.getContinuationToken();
        this.sequenceNumber = source.getSequenceNumber();
    }

}
