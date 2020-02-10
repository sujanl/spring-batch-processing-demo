package com.sujan.batches.controller;

import org.springframework.batch.core.*;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/job")
public class JobController {
    @Autowired
    JobLauncher jobLauncher;

    @Qualifier("csv2db")
    @Autowired
    Job csvToDbJob;

    @Qualifier("db2csv")
    @Autowired
    Job dbToCsvJob;

    @GetMapping("/csv-to-db")
    public String loadCSVTODBJob() throws JobParametersInvalidException,
            JobExecutionAlreadyRunningException,
            JobRestartException,
            JobInstanceAlreadyCompleteException {

        return this.executeJob(csvToDbJob);
    }

    @GetMapping("/db-to-csv")
    public String loadDbToCSVJob() throws
            JobParametersInvalidException,
            JobExecutionAlreadyRunningException,
            JobRestartException,
            JobInstanceAlreadyCompleteException {

        return this.executeJob(dbToCsvJob);
    }

    private String executeJob(Job dbToCsvJob) throws
            JobParametersInvalidException,
            JobExecutionAlreadyRunningException,
            JobRestartException,
            JobInstanceAlreadyCompleteException {
        Map<String, JobParameter> maps = new HashMap<>();
        maps.put("time", new JobParameter(System.currentTimeMillis()));
        JobParameters parameters = new JobParameters(maps);
        JobExecution jobExecution = jobLauncher.run(dbToCsvJob, parameters);

        System.out.println("JobExecution: " + jobExecution.getStatus());

        System.out.println("Batch is Running...");
        while (jobExecution.isRunning()) {
            System.out.println("...");
        }

//        return jobExecution.getStatus();
        return String.format("Job "+jobExecution.getJobInstance()+" submitted successfully."+jobExecution.getStatus().toString());
    }
}
