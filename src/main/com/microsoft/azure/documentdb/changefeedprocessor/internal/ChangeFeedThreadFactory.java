package com.microsoft.azure.documentdb.changefeedprocessor.internal;

import java.util.concurrent.ThreadFactory;

public class ChangeFeedThreadFactory implements ThreadFactory {

    private String ThreadSuffixName;

    public ChangeFeedThreadFactory(String ThreadSuffixName){
        this.ThreadSuffixName = ThreadSuffixName;
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread thread = new Thread(r);
        thread.setName(String.format("{0}_{1}",thread.getId(),this.ThreadSuffixName));
        thread.setDaemon(true);
        return thread;
    }
}
