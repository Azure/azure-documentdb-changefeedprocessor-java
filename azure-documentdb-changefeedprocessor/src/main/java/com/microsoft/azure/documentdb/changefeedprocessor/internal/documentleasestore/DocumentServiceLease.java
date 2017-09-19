/*
 * *
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

package java.com.microsoft.azure.documentdb.changefeedprocessor.internal.documentleasestore;

import java.com.microsoft.azure.documentdb.changefeedprocessor.internal.Lease;
import java.util.Date;

public class DocumentServiceLease extends Lease {

    private Date _unixStartTime = new Date();
    private String _id;
    public String _etag;
    public LeaseState _state;
    public Date _timestamp;
    private long _ts;

    public DocumentServiceLease()
    {
    }

    public DocumentServiceLease(DocumentServiceLease other)
    {
        super(other);
        this._id = other.getId();
        this._state = other.getState();
        this._etag = other.getEtag();
        this._ts = other.getTs();
    }

    public String ToString(){
        return String.format("{0} {1} Owner='{2}' Continuation={3} Timestamp(local)={4}",
                this.getId(),
                this.getState(),
                this.getOwner(),
                this.getContinuationToken(),
                this.getTimestamp());
    }

    public Date get_unixStartTime() {
        return _unixStartTime;
    }

    public void setUnixStartTime(Date _unixStartTime) {
        this._unixStartTime = _unixStartTime;
    }

    public String getId() {
        return _id;
    }

    public void setid(String _id) {
        this._id = _id;
    }

    public String getEtag() {
        return _etag;
    }

    public void setEtag(String _etag) {
        this._etag = _etag;
    }

    public LeaseState getState() {
        return _state;
    }

    public void setState(LeaseState _state) {
        this._state = _state;
    }

    public Date getTimestamp() {
        return _timestamp;
    }

    public void setTimestamp(Date _timestamp) {
        this._timestamp = _timestamp;
    }

    public long getTs() {
        return _ts;
    }

    public void setTs(long _ts) {
        this._ts = _ts;
    }
}
