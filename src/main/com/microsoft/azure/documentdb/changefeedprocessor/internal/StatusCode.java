/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.microsoft.azure.documentdb.changefeedprocessor.internal;

/**
 *
 * @author yoterada
 */
public enum StatusCode {

    /**
     * The operation is attempting to act on a resource that no longer exists. For example, the resource may have already been deleted.
     */
    NotFound ( 404 ),

    /**
     * The id provided for a resource on a PUT or POST operation has been taken by an existing resource.
     */

    Conflict (409),
    /**
     * The resource is gone
     */

    Gone (410),

    /**
     * The operation specified an eTag that is different from the version available at the server, i.e., an optimistic concurrency error.
     * Retry the request after reading the latest version of the resource and updating the eTag on the request.
     */

    PreconditionFailed(412),
    /**
     * The collection has exceeded the provisioned throughput limit. Retry the request after the server specified retry after duration.
     * For more information on DocumentDB performance levels, see DocumentDB levels.
     */

    TooManyRequests(429),

    /**
     * The operation could not be completed because the service was unavailable. This could happen due to network connectivity or service availability issues.
     * It is safe to retry the operation. If the issue persists, please contact support.
     */
    ServiceUnavailable(503);

    private int value;

    private StatusCode(int value) {
        this.value = value;
    }

    public int Value() {return this.value;}
}
