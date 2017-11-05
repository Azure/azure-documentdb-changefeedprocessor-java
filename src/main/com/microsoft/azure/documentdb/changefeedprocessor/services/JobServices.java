package com.microsoft.azure.documentdb.changefeedprocessor.services;

public class JobServices {
	// CR: 'async' implies returning a task. We should remove 'Async' from everywhere in the project.
	// CR: besides, we should use DocumentDB async (rx.java API) for async calls. Doesn't have to be fixed in 1st version though.
    public void runAsync(Job job, String initialData) {
        JobRunnable runnable = new JobRunnable(job, initialData);
        Thread thread = new Thread(runnable);
        thread.start();
    }
}
