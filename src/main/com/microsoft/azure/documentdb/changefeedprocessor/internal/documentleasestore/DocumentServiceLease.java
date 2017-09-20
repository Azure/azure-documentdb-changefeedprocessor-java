/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.microsoft.azure.documentdb.changefeedprocessor.internal.documentleasestore;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.Locale;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.changefeedprocessor.internal.Lease;

import lombok.Getter;
import lombok.Setter;

/**
 *
 * @author yoterada
 */
public class DocumentServiceLease extends Lease {

	private static final Instant unixStartTime = Instant.EPOCH;

    public DocumentServiceLease() { 
    }

    public DocumentServiceLease(DocumentServiceLease other) {
        super(other);
    	this.id = other.id;
        this.state = other.state;
        this.eTag = other.eTag;
        this.tS = other.tS;
    }

    public DocumentServiceLease(Document document) {
        super(fromDocument(document));
    	if (document == null) {
            throw new IllegalArgumentException("document");
        }
    }

    @JsonProperty("id")
    @Getter @Setter public String id;

    @JsonProperty("_etag")
    @Getter @Setter public String eTag;
    
    @JsonProperty("state")
    @Getter @Setter public LeaseState state;

    @JsonIgnore
    public Instant timestamp;

    @JsonIgnore
    public String concurrencyToken;
    
    @JsonProperty("_ts")
    @Getter @Setter private long tS;
        
    public Instant getTimestamp() {
    	return unixStartTime.plusSeconds(tS);
    }
    
    public void setTimestamp(Instant value) {
    	tS = Duration.between(value, unixStartTime).getSeconds(); 
    }    

    @Override
    public String getConcurrencyToken() {
    	return eTag;
    }

    @Override
    public String toString() {
        return String.format(
            Locale.ROOT,
            "{0} {1} Owner='{2}' Continuation={3} Timestamp(local)={4}",
            id,
            state,
            getOwner(),
            getContinuationToken(),
            LocalTime.from(timestamp));
    }

    private static DocumentServiceLease fromDocument(Document document) {
        ObjectMapper mapper = new ObjectMapper();
    	
        try {
        	String json = mapper.writeValueAsString(document);
			return mapper.readValue(json, DocumentServiceLease.class);
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
    } 
}
