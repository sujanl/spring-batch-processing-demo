package com.sujan.batches.job;

import com.sujan.batches.dto.EmployeeDto;
import com.sujan.batches.entity.Employee;
import com.sujan.batches.listner.CsvToDbListner;
import com.sujan.batches.processor.EmployeeDtoToEmployeeProcessor;
import com.sujan.batches.writer.EmployeeDBWriter;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.step.builder.AbstractTaskletStepBuilder;
import org.springframework.batch.core.step.skip.SkipLimitExceededException;
import org.springframework.batch.core.step.skip.SkipPolicy;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;

@Configuration
@EnableBatchProcessing
public class CSVToDBJobConfig {

    private JobBuilderFactory jobBuilderFactory;
    private StepBuilderFactory stepBuilderFactory;
    private EmployeeDtoToEmployeeProcessor employeeDtoToEmployeeProcessor;
    private EmployeeDBWriter employeeDBWriter;

    @Autowired
    public CSVToDBJobConfig(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory, EmployeeDtoToEmployeeProcessor employeeDtoToEmployeeProcessor, EmployeeDBWriter employeeDBWriter){
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
        this.employeeDtoToEmployeeProcessor = employeeDtoToEmployeeProcessor;
        this.employeeDBWriter = employeeDBWriter;
    }

    @Qualifier(value = "csv2db")
    @Bean
    public Job CsvToDb() throws Exception {
        return this.jobBuilderFactory.get("CSVToDB")
                .start(csvToDbStep())
                .listener(new CsvToDbListner())
                .build();
    }

    @Bean
    public Step csvToDbStep() throws Exception {
        return this.stepBuilderFactory.get("CSVToDB-step")
                .<EmployeeDto, Employee>chunk(500)
                .reader(employeeReader())
                .processor(employeeDtoToEmployeeProcessor)
                .writer(employeeDBWriter)
                .faultTolerant().skipPolicy(new SkipPolicy() {
                    @Override
                    public boolean shouldSkip(Throwable throwable, int failedCount) throws SkipLimitExceededException {
                        return (failedCount >= 5) ? false : true;//skip the fault job but if fault job exceed 5 stop job execution
                    }
                })
//                .taskExecutor(getTaskExecutor())

                .build();
    }


    @Bean
    public TaskExecutor getTaskExecutor() {
        SimpleAsyncTaskExecutor simpleAsyncTaskExecutor = new SimpleAsyncTaskExecutor();
        simpleAsyncTaskExecutor.setConcurrencyLimit(5);
        return simpleAsyncTaskExecutor;
    }

    @Bean
    @StepScope
    public FlatFileItemReader<EmployeeDto> employeeReader() throws Exception {
        FlatFileItemReader<EmployeeDto> reader = new FlatFileItemReader<>();
        reader.setResource(new FileSystemResource("src/main/resources/employees.csv"));
        reader.setName("Employee-Reader");
        reader.setLineMapper(lineMapper());
        return reader;
    }

    @Bean
    @StepScope
    public LineMapper<EmployeeDto> lineMapper() {
        DefaultLineMapper<EmployeeDto> defaultLineMapper = new DefaultLineMapper<>();

        defaultLineMapper.setLineTokenizer(getDelimitedLineTokenizer());
        defaultLineMapper.setFieldSetMapper(getFieldSetMapper());

        return defaultLineMapper;
    }

    @Bean
    @StepScope
    public FieldSetMapper<EmployeeDto> getFieldSetMapper() {
        BeanWrapperFieldSetMapper<EmployeeDto> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(EmployeeDto.class);
        return fieldSetMapper;
    }

    @Bean
    @StepScope
    public DelimitedLineTokenizer getDelimitedLineTokenizer(){
        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();

        lineTokenizer.setDelimiter(",");
        lineTokenizer.setStrict(false);
        lineTokenizer.setNames(new String[]{"employeeId", "firstName", "lastName", "email", "age"});

        return lineTokenizer;
    }

}
