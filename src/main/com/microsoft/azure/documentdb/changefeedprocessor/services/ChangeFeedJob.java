package com.microsoft.azure.documentdb.changefeedprocessor.services;

public class ChangeFeedJob implements Job {
    @Override
    public void start(Object initialData) {

//        service = new DocumentServices(docInfo);
//        DocumentServicesClient client =  service.createClient();

        while(true) {
            System.out.println("running");
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void stop() {
        System.out.println("stopped");
    }
}
