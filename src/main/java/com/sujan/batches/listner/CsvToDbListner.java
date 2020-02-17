package com.sujan.batches.listner;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;

public class CsvToDbListner implements JobExecutionListener {
    @Override
    public void beforeJob(JobExecution jobExecution) {
        System.out.println("Before the job 'CsvToDb' executes, job status: "+ jobExecution.getStatus());
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        System.out.println("Before the job 'CsvToDb' executes, job status: "+jobExecution.getStatus());
    }
}
