package com.sujan.batches.job;

import com.sujan.batches.dto.EmployeeDto;
import com.sujan.batches.entity.Employee;
import com.sujan.batches.mapper.EmployeeDBRowMapper;
import com.sujan.batches.processor.EmployeeToEmployeeDtoProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;

import javax.sql.DataSource;

@Configuration
@EnableBatchProcessing
public class DBToCSVJobConfig {

    private static final  Logger logger = LoggerFactory.getLogger(DBToCSVJobConfig.class);

    private JobBuilderFactory jobBuilderFactory;
    private StepBuilderFactory stepBuilderFactory;
    private DataSource dataSource;
    private EmployeeToEmployeeDtoProcessor employeeToEmployeeDtoProcessor;

    public DBToCSVJobConfig(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory, DataSource dataSource, EmployeeToEmployeeDtoProcessor employeeToEmployeeDtoProcessor) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
        this.dataSource = dataSource;
        this.employeeToEmployeeDtoProcessor = employeeToEmployeeDtoProcessor;
    }

    @Bean
    @Qualifier(value = "db2csv")
    public Job dbToCsvJob() throws Exception {
        return jobBuilderFactory.get("DBToCSV-Job")
                .start(dbToCsvStep())
                .build();
    }

    @Bean
    public Step dbToCsvStep() throws Exception {
        return stepBuilderFactory.get("DBToCSV-Step")
                .<Employee, EmployeeDto>chunk(50)
                .reader(employeeDBReader())
                .processor(employeeToEmployeeDtoProcessor)
                .writer(employeeCSVWriter())
                .build();
    }

    @Bean
    public ItemWriter<EmployeeDto> employeeCSVWriter() throws Exception {
        FlatFileItemWriter<EmployeeDto> writer = new FlatFileItemWriter<>();
        writer.setResource(new FileSystemResource("output/employee_output.csv"));
        writer.setLineAggregator(new DelimitedLineAggregator<EmployeeDto>() {{
                setFieldExtractor(new BeanWrapperFieldExtractor<EmployeeDto>() {{
                    setNames(new String[]{"employeeId", "firstName", "lastName", "email", "age"});
                }});
            }
        });
        writer.setShouldDeleteIfExists(true);
        return writer;
    }

    @Bean
    public ItemStreamReader<Employee> employeeDBReader() {
        JdbcCursorItemReader<Employee> reader = new JdbcCursorItemReader<>();
        reader.setDataSource(dataSource);
        reader.setSql("select * from employee");
        reader.setRowMapper(new EmployeeDBRowMapper());
        return reader;
    }
}
