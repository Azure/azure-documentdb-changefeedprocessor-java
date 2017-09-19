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

package java.com.microsoft.azure.documentdb.changefeedprocessor.internal;

public abstract class Lease {


    private String _partitionId;
    private String _owner;
    private String _continuationToken;
    private long _sequenceNumber;
    private String _concurrencyToken;



    public Lease()
    {
    }

    public Lease(Lease source)
    {
        this._partitionId = source.getPartitionId();
        this._owner = source.getOwner();
        this._continuationToken = source.getContinuationToken();
        this._sequenceNumber = source.getSequenceNumber();
    }


    public boolean Equals(Object obj)
    {
        if (obj == this)
        {
            return true;
        }

        Lease lease = (Lease)obj ;
        if (lease == null)
        {
            return false;
        }

        return this.getPartitionId().equals(lease.getPartitionId());
    }

    public int GetHashCode()
    {
        return this.getPartitionId().hashCode();
    }

    public String getPartitionId() {
        return _partitionId;
    }

    public void setPartitionId(String _partitionId) {
        this._partitionId = _partitionId;
    }

    public String getOwner() {
        return _owner;
    }

    public void setOwner(String _owner) {
        this._owner = _owner;
    }

    public String getContinuationToken() {
        return _continuationToken;
    }

    public void setContinuationToken(String _continuationToken) {
        this._continuationToken = _continuationToken;
    }

    public long getSequenceNumber() {
        return _sequenceNumber;
    }

    public void setSequenceNumber(long _sequenceNumber) {
        this._sequenceNumber = _sequenceNumber;
    }

    public String getConcurrencyToken() {
        return _concurrencyToken;
    }

    public void setConcurrencyToken(String _concurrencyToken) {
        this._concurrencyToken = _concurrencyToken;
    }
}
