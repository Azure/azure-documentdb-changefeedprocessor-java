/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.microsoft.azure.documentdb.changefeedprocessor.internal.documentleasestore;

import java.util.Date;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author yoterada
 */
public class PartitionInfoTest {
    
    public PartitionInfoTest() {
    }

    /**
     * Test of lastExcetution method, of class PartitionInfo.
     */
    @Test
    public void testLastExcetution() {
        System.out.println("lastExcetution");
        PartitionInfo instance = new PartitionInfo();
        Date expResult = null;
        Date result = instance.lastExcetution();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of updateExecution method, of class PartitionInfo.
     */
    @Test
    public void testUpdateExecution() {
        System.out.println("updateExecution");
        PartitionInfo instance = new PartitionInfo();
        instance.updateExecution();
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of key method, of class PartitionInfo.
     */
    @Test
    public void testKey() {
        System.out.println("key");
        PartitionInfo instance = new PartitionInfo();
        String expResult = "";
        String result = instance.key();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }
    
}
