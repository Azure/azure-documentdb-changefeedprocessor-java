/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.microsoft.azure.documentdb.changefeedprocessor.internal;

import com.microsoft.azure.documentdb.changefeedprocessor.IChangeFeedObserver;

/**
 *
 * @author yoterada
 */
// CR: looks like this is not used. Remove.
public class DocumentFeedObserverFactory<T extends IChangeFeedObserver> implements IDocumentFeedObserverFactory {

    public IChangeFeedObserver createObserver()
    {
        return null;
    }
}
