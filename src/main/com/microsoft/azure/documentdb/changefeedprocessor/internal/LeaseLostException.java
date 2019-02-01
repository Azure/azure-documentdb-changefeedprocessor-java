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
package com.microsoft.azure.documentdb.changefeedprocessor.internal;

public class LeaseLostException extends Exception {

    private Lease lease;
    private Boolean isGone;

    public LeaseLostException()
    {
    }

    public LeaseLostException(Lease lease)
    {
        setLease(lease);
    }

    public LeaseLostException(Lease lease, Exception innerException)
    {
        setLease(lease);
        setIsGone(false);
    }

    public LeaseLostException(Lease lease, Exception innerException, Boolean isGone)
    {
    	setLease(lease);
        setIsGone(isGone);
    }

    public LeaseLostException(String message)
    {
    	new Exception(message);
    }

    public LeaseLostException(String message, Exception innerException)
    {
    	new Exception(message, innerException);
    }

    public Lease getLease() {
        return lease;
    }
    private void setLease(Lease value) {
    	this.lease = value;
    }

    public Boolean geteIsGone() {
        return isGone;
    }

    public void setIsGone(Boolean value) {
    	this.isGone = value;
    }
}
