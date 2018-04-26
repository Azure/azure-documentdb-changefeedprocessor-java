/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
//package com.microsoft.azure.documentdb.changefeedprocessor.internal;
package com.microsoft.azure.documentdb.changefeedprocessor;

/**
*
* @author rogirdh
*/

interface IPartitionManager<T extends Lease> {
    void start();
    void stop();
    AutoCloseable subscribe(IPartitionObserver<T> observer);
}
