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

public enum StatusCode {

    /**
     * The operation is attempting to act on a resource that no longer exists. For example, the resource may have already been deleted.
     */
    NOTFOUND( 404 ),

    /**
     * The id provided for a resource on a PUT or POST operation has been taken by an existing resource.
     */
    CONFLICT(409),

    /**
     * The resource is gone
     */
    GONE(410),

    /**
     * The operation specified an eTag that is different from the version available at the server, i.e., an optimistic concurrency error.
     * Retry the request after reading the latest version of the resource and updating the eTag on the request.
     */
    PRECONDITION_FAILED(412),

    /**
     * The collection has exceeded the provisioned throughput limit. Retry the request after the server specified retry after duration.
     * For more information on DocumentDB performance levels, see DocumentDB levels.
     */
    TOO_MANY_REQUESTS(429),

    /**
     * The operation could not be completed because the service was unavailable. This could happen due to network connectivity or service availability issues.
     * It is safe to retry the operation. If the issue persists, please contact support.
     */
    SERVICE_UNAVAILABLE(503);

    private final int value;

    private StatusCode(int value) {
        this.value = value;
    }

    public int Value() {return this.value;}
}
