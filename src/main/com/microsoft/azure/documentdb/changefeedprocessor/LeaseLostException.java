/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
//package com.microsoft.azure.documentdb.changefeedprocessor.internal;
package com.microsoft.azure.documentdb.changefeedprocessor;

class LeaseLostException extends Exception {

	private static final long serialVersionUID = 1L;
	private Lease lease;
    private Boolean isGone;

    public LeaseLostException()
    {
    }

    public LeaseLostException(Lease lease)
    {
        setLease(lease);
    }

    public LeaseLostException(Lease lease, Exception innerException)
    {
        setLease(lease);
        setIsGone(false);
    }

    public LeaseLostException(Lease lease, Exception innerException, Boolean isGone)
    {
    	setLease(lease);
        setIsGone(isGone);
    }

    public LeaseLostException(String message)
    {
    	new Exception(message);
    }

    public LeaseLostException(String message, Exception innerException)
    {
    	new Exception(message, innerException);
    }

    public Lease getLease() {
        return lease;
    }
    
    private void setLease(Lease value) {
    	this.lease = value;
    }

    public Boolean geteIsGone() {
        return isGone;
    }

    private void setIsGone(Boolean value) {
    	this.isGone = value;
    }
}
