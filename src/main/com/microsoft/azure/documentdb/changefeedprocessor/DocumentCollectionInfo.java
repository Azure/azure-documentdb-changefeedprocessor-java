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
package com.microsoft.azure.documentdb.changefeedprocessor;

import com.microsoft.azure.documentdb.ConnectionMode;
import com.microsoft.azure.documentdb.ConnectionPolicy;

import java.net.URI;

public class DocumentCollectionInfo {

    private URI uri;
    private String masterKey;
    private String databaseName;
    private String collectionName;
    private ConnectionPolicy connectionPolicy;

    public DocumentCollectionInfo(){

        connectionPolicy = new ConnectionPolicy();
        connectionPolicy.setConnectionMode(ConnectionMode.DirectHttps);

    }


    /**
     * Instantiante a new instance of {@link DocumentCollectionInfo} using other as a source
     * @param other the other {@link DocumentCollectionInfo} object that will be used as source
     */
    public DocumentCollectionInfo(DocumentCollectionInfo other)
    {
        this.uri = other.getUri();
        this.masterKey = other.getMasterKey();
        this.databaseName = other.getDatabaseName();
        this.collectionName = other.getCollectionName();
        this.connectionPolicy = other.getConnectionPolicy();
    }

    /**
     * Get the URI of Document Service
     * @return
     */
    public URI getUri() {
        return uri;
    }

    /**
     * Sets the URI of Document Service
     * @param _uri
     */
    public void setUri(URI _uri) {
        this.uri = _uri;
    }

    /**
     * Get the Masterkey of Document Service
     * @return
     */
    public String getMasterKey() {
        return masterKey;
    }

    /**
     * Set the MasterKey of Document Service
     * @param _masterKey
     */
    public void setMasterKey(String _masterKey) {
        this.masterKey = _masterKey;
    }

    /**
     * Get the Database name
     * @return
     */
    public String getDatabaseName() {
        return databaseName;
    }

    /**
     * Set the Database Name
     * @param _databaseName
     */
    public void setDatabaseName(String _databaseName) {
        this.databaseName = _databaseName;
    }

    /**
     * Get the Collection Name
     * @return
     */
    public String getCollectionName() {
        return collectionName;
    }

    /**
     * Set the Collection Name
     * @param _collectionName
     */
    public void setCollectionName(String _collectionName) {
        this.collectionName = _collectionName;
    }

    /**
     * Get the Connection Policy
     * @return
     */
    public ConnectionPolicy getConnectionPolicy() {
        return connectionPolicy;
    }

    /**
     * Set Connection policy
     * @param _connectionPolicy
     */
    public void setConnectionPolicy(ConnectionPolicy _connectionPolicy) {
        this.connectionPolicy = _connectionPolicy;
    }

}
