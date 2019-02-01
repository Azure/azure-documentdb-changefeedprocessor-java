/**
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
 */
package com.microsoft.azure.documentdb.changefeedprocessor.internal.documentleasestore;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.Lease;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalTime;
import java.util.Locale;

public class DocumentServiceLease extends Lease {
	
	private static final Instant unixStartTime = Instant.EPOCH;

    public DocumentServiceLease() { 
    }

    public DocumentServiceLease(DocumentServiceLease other) {
        super(other);
    	this.id = other.id;
        this.state = other.state;
        this.eTag = other.eTag;
        this.ts = other.ts;
    }

    public DocumentServiceLease(Document document) {
    	this(fromDocument(document));
        
    	if (document == null) {
            throw new IllegalArgumentException("document");
        }      
    }

    @JsonProperty("id")
    public String id;

    @JsonProperty("_etag")
    public String eTag;
    
    @JsonProperty("state")
    public LeaseState state;

    @JsonIgnore
    public Instant timestamp;

    @JsonIgnore
    public String concurrencyToken;
    
    @JsonProperty("_ts")
    private long ts;

    /**
     * Gets the ID associated with the resource.
     *
     * @return the ID associated with the resource.
     */
    public String getId() {
        return this.id;
    }

    /**
     * Sets the ID of the resource.
     *
     * @param id the ID of the resource.
     */
    public void setId(String id) {
        this.id = id;
    }

    /**
     * Gets the eTag associated with the resource.
     *
     * @return the eTag associated with the resource.
     */
    public String getETag() {
        return this.eTag;
    }

    /**
     * Sets the eTag of the resource.
     *
     * @param eTag the eTag of the resource.
     */
    public void setETag(String eTag) {
        this.eTag = eTag;
    }

    /**
     * Gets the lease state associated with the resource.
     *
     * @return the lease state associated with the resource.
     */
    public LeaseState getState() {
        return this.state;
    }

    /**
     * Sets the lease state of the resource.
     *
     * @param state the lease state of the resource.
     */
    public void setState(LeaseState state) {
        this.state = state;
    }

    /**
     * Gets the ts associated with the resource.
     *
     * @return the ts associated with the resource.
     */
    public long getTs() {
        return this.ts;
    }

    public Instant getTimestamp() {
    	return unixStartTime.plusSeconds(ts);
    }
    
    public void setTimestamp(Instant value) {
    	ts = Duration.between(value, unixStartTime).getSeconds(); 
    }    

    public String getConcurrencyToken() {
    	return eTag;
    }

    @Override
    public String toString() {
        return String.format(
            Locale.ROOT,
            "%d %s Owner='%s' Continuation=%s Timestamp(local)=%s",
            id,
            state,
            this.getOwner(),
            this.getContinuationToken(),
            LocalTime.from(timestamp));
    }

    private static DocumentServiceLease fromDocument(Document document) {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);

		try {
			String json = document.toJson();
			return mapper.readValue(json, DocumentServiceLease.class);
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
    } 
}
