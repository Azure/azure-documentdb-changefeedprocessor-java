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
package com.microsoft.azure.documentdb.changefeedprocessor.services;

import com.microsoft.azure.documentdb.DocumentClientException;

import java.util.logging.Logger;

public class JobRunnable implements Runnable {

    private final Job job;
    private String initialData;
    private Logger logger = Logger.getLogger(JobRunnable.class.getName());

    public JobRunnable(Job job, String initialData) {
        this.job = job;
        this.initialData = initialData;

        logger.info("Constructor JobRunnable");
    }

    @Override
    public void run() {
        try {
            logger.info(String.format("Running Job: %s - InitialData: %s", job, initialData));
            job.start(initialData);
        } catch (DocumentClientException e) {
            logger.severe(e.getMessage());
            e.printStackTrace();
        } catch (InterruptedException e) {
            logger.severe(e.getMessage());
            e.printStackTrace();
        }
    }
}
