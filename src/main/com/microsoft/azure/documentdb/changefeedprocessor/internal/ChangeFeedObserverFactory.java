/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.microsoft.azure.documentdb.changefeedprocessor.internal;

import com.microsoft.azure.documentdb.changefeedprocessor.ChangeFeedObserverInterface;
import com.microsoft.azure.documentdb.changefeedprocessor.ChangeFeedObserverFactoryInterface;


public class ChangeFeedObserverFactory<T extends ChangeFeedObserverInterface> implements ChangeFeedObserverFactoryInterface {
    private final Class<T> type;

    // CR: why is T template parameter never used? Can we use it instead of passing Class othwerwise it's not a template really?
    public ChangeFeedObserverFactory(Class<T> type) {
        this.type = type;
    }

    @SuppressWarnings("deprecation")
	@Override
    public ChangeFeedObserverInterface createObserver() throws IllegalAccessException, InstantiationException {
        return (ChangeFeedObserverInterface) type.newInstance();
    }
}