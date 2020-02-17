package com.sujan.batches.scheduler;

import com.sujan.batches.job.CSVToDBJobConfig;
import org.springframework.batch.core.*;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class JobScheduler {
    @Autowired
    JobLauncher jobLauncher;

    @Qualifier("csv2db")
    @Autowired
    Job csvToDbJob;

    @Scheduled(cron = "0 0/2 * 1/1 * ?")
//    @Scheduled(fixedDelay = 30000)//30sec
    public void scheduleJob() throws JobParametersInvalidException, JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException {
        System.out.println("Job triggered!!!");
        this.runJob();
    }

    private void runJob() throws JobParametersInvalidException, JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException {
        System.out.println("Batch is Running by Scheduler...");
        Map<String, JobParameter> maps = new HashMap<>();
        maps.put("time", new JobParameter(System.currentTimeMillis()));
        JobParameters parameters = new JobParameters(maps);
        JobExecution jobExecution = jobLauncher.run(csvToDbJob, parameters);
        System.out.println("Job "+jobExecution.getJobInstance()+" submitted successfully."+jobExecution.getStatus().toString());
    }
}
