package com.sujan.batches;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@EnableBatchProcessing
public class BatchesApplication {

    public static void main(String[] args) {
        SpringApplication.run(BatchesApplication.class, args);
    }

}
