/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.microsoft.azure.documentdb.changefeedprocessor.internal;

import com.microsoft.azure.documentdb.changefeedprocessor.IChangeFeedObserver;
import com.microsoft.azure.documentdb.changefeedprocessor.IChangeFeedObserverFactory;
/**
 *
 * @author yoterada
 */
public class ChangeFeedObserverFactory implements IChangeFeedObserverFactory {

    private final Class _type;

    public ChangeFeedObserverFactory(Class type) {
        this._type = type;
    }

    public IChangeFeedObserver createObserver() {

        IChangeFeedObserver newInstance = null;

        try {
            newInstance = (IChangeFeedObserver)_type.newInstance();
        }
        catch(IllegalAccessException e) {
        }
        catch(InstantiationException e) {
        }

        return newInstance;
    }
}