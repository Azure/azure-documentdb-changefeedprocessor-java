/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.microsoft.azure.documentdb.changefeedprocessor;

/**
 *
 * @author fcatae
 */
enum SubStatusCode {
    /**
     * 410: partition key range is gone
     */
    PartitionKeyRangeGone(1002),
    
    /**
     * 410: partition splitting
     */
    Splitting (1007),
    
    /**
     * 404: LSN in session token is higher
     */
    ReadSessionNotAvailable(1002);
	
    private int value;

    private SubStatusCode(int value) {
        this.value = value;
    }

    public int Value() { return this.value; }
}
