package com.example.demo;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JpaPagingItemReader;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.LineAggregator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Repository;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import javax.persistence.*;
import javax.persistence.Entity;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.lang.System.*;

@SpringBootApplication
public class DemoApplication {
    public static void main(String[] args) {
        SpringApplication.run(DemoApplication.class, args);
    }
}

@RestController
@RequiredArgsConstructor
class LoadController{

    private final JobLauncher jobLauncher;

    private final Job job1;

    private final Job job2;


    @GetMapping("/{id}")
    public BatchStatus load(@PathVariable String id) throws JobParametersInvalidException, JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException {
        Map<String, JobParameter> params = new HashMap<>();

        params.put("triggeredTime", new JobParameter(currentTimeMillis()));

        JobParameters jobParameters = new JobParameters(params);

        Job job;
        if("1".equals(id)){
            job=job1;
        }else {
            job=job2;
        }
        JobExecution jobExecution = jobLauncher.run(job, jobParameters);

        while (jobExecution.isRunning()){

        }

        return jobExecution.getStatus();
    }
}

@Component
class UserItemProcessor implements ItemProcessor<User, User>{
    private static final Map<String, String> DEPT_MAP;

    static {
        DEPT_MAP = new HashMap<>();

        DEPT_MAP.put("001", "Technology");
        DEPT_MAP.put("002", "Operations");
        DEPT_MAP.put("003", "Accounts");
    }

    @Override
    public User process(User user) throws Exception {
        user.setDept(DEPT_MAP.get(user.getDept()));

        return user;
    }
}


@Component
@RequiredArgsConstructor
class DbWriter implements ItemWriter<User>{

    private final UserRepository userRepository;

    @Override
    public void write(List<? extends User> list) throws Exception {
        userRepository.save(list);
        userRepository.flush();
    }
}

@Component
class FileItemWriter extends FlatFileItemWriter<User>{

    @Value("${output.file}")
    private String path;

    private LineAggregator lineAggregator;

    private Resource resource;


    public FileItemWriter(@Autowired LineAggregator lineAggregator, @Value("${output.file}") Resource resource){
        super.setLineAggregator(lineAggregator);
        super.setResource(resource);

        this.lineAggregator=lineAggregator;

        this.resource = resource;
    }

    @Override
    public void write(List<? extends User> items) throws Exception {
        FlatFileItemWriter<User> flatFileItemWriter = new FlatFileItemWriter<>();

        flatFileItemWriter.setResource(resource);

        flatFileItemWriter.setLineAggregator(lineAggregator);
        flatFileItemWriter.setLineSeparator("\n");
        flatFileItemWriter.setName("file-writer");
        flatFileItemWriter.open(new ExecutionContext());
        flatFileItemWriter.write(items);
    }

}

@Repository
interface UserRepository extends JpaRepository<User, Long>{

}
