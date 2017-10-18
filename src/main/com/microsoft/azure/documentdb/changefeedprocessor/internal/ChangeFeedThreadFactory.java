package com.microsoft.azure.documentdb.changefeedprocessor.internal;

import java.util.concurrent.ThreadFactory;
import java.util.logging.Logger;

public class ChangeFeedThreadFactory implements ThreadFactory {

    private String ThreadSuffixName;
    private Logger logger = Logger.getLogger(ChangeFeedThreadFactory.class.getName());

    public ChangeFeedThreadFactory(String ThreadSuffixName){
        this.ThreadSuffixName = ThreadSuffixName;
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread thread = new Thread(r);
        thread.setName(String.format("%s_%s",thread.getId(),this.ThreadSuffixName));
        thread.setDaemon(false);

        logger.info(String.format("Thread Created Name: %s, ID: %s, Daemon: %s", thread.getName(), thread.getId(), thread.isDaemon()));

        return thread;
    }
}
