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
package com.microsoft.azure.documentdb.changefeedprocessor.internal;

public abstract class Lease {

    private String partitionId;
    private String owner;
    private String continuationToken;
    private long sequenceNumber;
    private String concurrencyToken;

    /**
     * Gets the partition ID associated with the resource.
     *
     * @return the partition ID associated with the resource.
     */
    public String getPartitionId() {
        return this.partitionId;
    }

    /**
     * Sets the partitionId of the resource.
     *
     * @param partitionId the partitionId of the resource.
     */
    public void setPartitionId(String partitionId) {
        this.partitionId = partitionId;
    }

    /**
     * Gets the owner associated with the resource.
     *
     * @return the owner associated with the resource.
     */
    public String getOwner() {
        return this.owner;
    }

    /**
     * Sets the owner of the resource.
     *
     * @param owner the owner of the resource.
     */
    public void setOwner(String owner) {
        this.owner = owner;
    }

    /**
     * Gets the continuation token associated with the resource.
     *
     * @return the continuation token associated with the resource.
     */
    public String getContinuationToken() {
        return this.continuationToken;
    }

    /**
     * Sets the continuation token of the resource.
     *
     * @param continuationToken the continuation token of the resource.
     */
    public void setContinuationToken(String continuationToken) {
        this.continuationToken = continuationToken;
    }

    /**
     * Gets the sequence number associated with the resource.
     *
     * @return the sequence number associated with the resource.
     */
    public long getSequenceNumber() {
        return this.sequenceNumber;
    }

    /**
     * Sets the sequence number of the resource.
     *
     * @param sequenceNumber the sequence number of the resource.
     */
    public void setSequenceNumber(long sequenceNumber) {
        this.sequenceNumber = sequenceNumber;
    }

    /**
     * Gets the concurrency token associated with the resource.
     *
     * @return the concurrency token associated with the resource.
     */
    public String getConcurrencyToken() {
        return this.concurrencyToken;
    }

    /**
     * Sets the concurrency token of the resource.
     *
     * @param concurrencyToken the concurrency token of the resource.
     */
    public void setConcurrencyToken(String concurrencyToken) {
        this.concurrencyToken = concurrencyToken;
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

    @Override public boolean equals(Object o) {
        if (o == this) return true;
        if (!(o instanceof Lease)) return false;
        Lease other = (Lease) o;
        return this.partitionId == null ? other.partitionId == null : this.partitionId.equals(other.partitionId);
    }

    @Override public int hashCode() {
        return this.partitionId.hashCode();
    }
}
