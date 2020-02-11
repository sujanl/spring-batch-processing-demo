package com.sujan.batches.job;

import com.sujan.batches.dto.EmployeeDto;
import com.sujan.batches.entity.Employee;
import com.sujan.batches.processor.EmployeeDtoToEmployeeProcessor;
import com.sujan.batches.writer.EmployeeDBWriter;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.file.FlatFileItemReader;
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
                .build();
    }

    @Bean
    public Step csvToDbStep() throws Exception {
        return this.stepBuilderFactory.get("CSVToDB-step")
                .<EmployeeDto, Employee>chunk(50)
                .reader(employeeReader())
                .processor(employeeDtoToEmployeeProcessor)
                .writer(employeeDBWriter)
                .build();
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
    public LineMapper<EmployeeDto> lineMapper() {
        DefaultLineMapper<EmployeeDto> defaultLineMapper = new DefaultLineMapper<>();

        defaultLineMapper.setLineTokenizer(getDelimitedLineTokenizer());
        defaultLineMapper.setFieldSetMapper(getFieldSetMapper());

        return defaultLineMapper;
    }

    @Bean
    public FieldSetMapper<EmployeeDto> getFieldSetMapper() {
        BeanWrapperFieldSetMapper<EmployeeDto> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(EmployeeDto.class);
        return fieldSetMapper;
    }

    @Bean
    public DelimitedLineTokenizer getDelimitedLineTokenizer(){
        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();

        lineTokenizer.setDelimiter(",");
        lineTokenizer.setStrict(false);
        lineTokenizer.setNames(new String[]{"employeeId", "firstName", "lastName", "email", "age"});

        return lineTokenizer;
    }

}
