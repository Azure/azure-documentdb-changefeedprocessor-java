/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.microsoft.azure.documentdb.changefeedprocessor;

public interface ChangeFeedObserverFactoryInterface {
	
	/// <summary>
    /// Creates a new instance of <see cref="DocumentDB.ChangeFeedProcessor.IChangeFeedObserver"/>.
    /// </summary>
    /// <returns>Created instance of <see cref="DocumentDB.ChangeFeedProcessor.IChangeFeedObserver"/>.</returns>
    ChangeFeedObserverInterface createObserver() throws IllegalAccessException, InstantiationException;
}
