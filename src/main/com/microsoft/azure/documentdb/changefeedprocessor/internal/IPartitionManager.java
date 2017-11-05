/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.microsoft.azure.documentdb.changefeedprocessor.internal;

/**
*
* @author rogirdh
*/
// CR: are TODOs still need to be addressed?
public interface IPartitionManager<T extends Lease> {
    void start();	//TODO: implement as async 
    void stop();	//TODO: implement as async 
    IDisposable subscribe(IPartitionObserver<T> observer);
}
