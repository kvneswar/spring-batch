package com.example.helloworld.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.flow.JobExecutionDecider;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.JobStepBuilder;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

@Slf4j
@Configuration
public class NestedJobConfig {

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Autowired
    private JobLauncher jobLauncher;

    @Bean
    public Step childStep(){
        return stepBuilderFactory.get("childStep")
                .tasklet((contribution, chunkContext) -> {
                    log.info("Child Step");
                    return RepeatStatus.FINISHED;
                })
                .build();
    }

    @Bean
    public Job childJob(){
        return jobBuilderFactory.get("childJob")
                .start(childStep())
                .build();
    }

    @Bean
    public Job parentJob(JobRepository jobRepository, PlatformTransactionManager platformTransactionManager){

        Step childJobStep = new JobStepBuilder(new StepBuilder("childJobStep"))
            .job(childJob())
            .launcher(jobLauncher)
            .repository(jobRepository)
            .transactionManager(platformTransactionManager)
            .build();

        return jobBuilderFactory.get("parentJob")
            .start(childJobStep)
            .build();
    }


}
