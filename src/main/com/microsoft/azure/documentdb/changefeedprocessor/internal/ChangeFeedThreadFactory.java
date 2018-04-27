package com.microsoft.azure.documentdb.changefeedprocessor.internal;

import java.util.concurrent.ThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChangeFeedThreadFactory implements ThreadFactory {

    private String threadSuffixName;
    private Logger logger = LoggerFactory.getLogger(ChangeFeedThreadFactory.class.getName());

    public ChangeFeedThreadFactory(String threadSuffixName){
        this.threadSuffixName = threadSuffixName;
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread thread = new Thread(r);
        thread.setName(String.format("%s_%s", thread.getId(), this.threadSuffixName));
        thread.setDaemon(true);

        logger.info(String.format("Thread Created Name: %s, ID: %s, Daemon: %s", thread.getName(), thread.getId(), thread.isDaemon()));

        return thread;
    }
}
