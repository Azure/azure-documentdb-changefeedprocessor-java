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

package java.com.microsoft.azure.documentdb.changefeedprocessor;

import com.microsoft.azure.documentdb.ConnectionMode;
import com.microsoft.azure.documentdb.ConnectionPolicy;

import java.net.URI;

public class DocumentCollectionInfo {

    private URI _uri;
    private String _masterKey;
    private String _databaseName;
    private String _collectionName;
    private ConnectionPolicy _connectionPolicy;

    public DocumentCollectionInfo(){

        _connectionPolicy = new ConnectionPolicy();
        _connectionPolicy.setConnectionMode(ConnectionMode.DirectHttps);

    }


    /**
     * Instantiante a new instance of {@link DocumentCollectionInfo} using other as a source
     * @param other the other {@link DocumentCollectionInfo} object that will be used as source
     */
    public DocumentCollectionInfo(DocumentCollectionInfo other)
    {
        this._uri = other.getUri();
        this._masterKey= other.getMasterKey();
        this._databaseName= other.getDatabaseName();
        this._collectionName= other.getCollectionName();
        this._connectionPolicy = other.getConnectionPolicy();
    }

    /**
     * Get the URI of Document Service
     * @return
     */
    public URI getUri() {
        return _uri;
    }

    /**
     * Sets the URI of Document Service
     * @param _uri
     */
    public void seUri(URI _uri) {
        this._uri = _uri;
    }

    /**
     * Get the Masterkey of Document Service
     * @return
     */
    public String getMasterKey() {
        return _masterKey;
    }

    /**
     * Set the MasterKey of Document Service
     * @param _masterKey
     */
    public void setMasterKey(String _masterKey) {
        this._masterKey = _masterKey;
    }

    /**
     * Get the Database name
     * @return
     */
    public String getDatabaseName() {
        return _databaseName;
    }

    /**
     * Set the Database Name
     * @param _databaseName
     */
    public void setDatabaseName(String _databaseName) {
        this._databaseName = _databaseName;
    }

    /**
     * Get the Collection Name
     * @return
     */
    public String getCollectionName() {
        return _collectionName;
    }

    /**
     * Set the Collection Name
     * @param _collectionName
     */
    public void setCollectionName(String _collectionName) {
        this._collectionName = _collectionName;
    }

    /**
     * Get the Connection Policy
     * @return
     */
    public ConnectionPolicy getConnectionPolicy() {
        return _connectionPolicy;
    }

    /**
     * Set Connection policy
     * @param _connectionPolicy
     */
    public void setConnectionPolicy(ConnectionPolicy _connectionPolicy) {
        this._connectionPolicy = _connectionPolicy;
    }

}
