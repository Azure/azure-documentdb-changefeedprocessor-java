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
package com.microsoft.azure.documentdb.changefeedprocessor.internal.documentleasestore;

import java.util.Date;
import org.junit.Test;
import static org.junit.Assert.*;

public class PartitionInfoTest {
    
    public PartitionInfoTest() {
    }

    /**
     * Test of lastExecution method, of class PartitionInfo.
     */
    @Test
    public void testLastExecution() {
        System.out.println("lastExecution");
        PartitionInfo instance = new PartitionInfo();
        Date expResult = null;
        Date result = instance.lastExecution();
        assertEquals(expResult, result);
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
        fail("The test case is a prototype.");
    }
    
}
