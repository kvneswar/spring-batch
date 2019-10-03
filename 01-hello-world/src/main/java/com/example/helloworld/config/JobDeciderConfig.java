package com.example.helloworld.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.flow.FlowExecutionStatus;
import org.springframework.batch.core.job.flow.JobExecutionDecider;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

//@Configuration
@Slf4j
public class JobDeciderConfig {

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Bean
    public Step firstStep(){

        Tasklet tasklet = (contribution, chunkContext) -> {
            log.info("This is the first step");

            return RepeatStatus.FINISHED;
        };

        return stepBuilderFactory.get("firstStep")
                .tasklet(tasklet)
                .build();
    }

    @Bean
    public Step evenStep(){

        Tasklet tasklet = (contribution, chunkContext) -> {
            log.info("This is the Even Step");

            return RepeatStatus.FINISHED;
        };

        return stepBuilderFactory.get("evenStep")
                .tasklet(tasklet)
                .build();
    }

    @Bean
    public Step oddStep(){

        Tasklet tasklet = (contribution, chunkContext) -> {
            log.info("This is the Odd Step");

            return RepeatStatus.FINISHED;
        };

        return stepBuilderFactory.get("oddStep")
                .tasklet(tasklet)
                .build();
    }


    int count = 0;

    JobExecutionDecider jobExecutionDecider = (jobExecution, stepExecution) -> {
        count++;

        if(count % 2 == 0){
            return new FlowExecutionStatus("EVEN");
        }else{
            return new FlowExecutionStatus("ODD");
        }

    };


    @Bean
    public Job job(){
        return jobBuilderFactory.get("deciderJob")
                .start(firstStep())
                .next(jobExecutionDecider)
                .from(jobExecutionDecider).on("ODD").to(oddStep())
                .from(jobExecutionDecider).on("EVEN").to(evenStep())
                .from(oddStep()).on("*").to(jobExecutionDecider)
                .end()
                .build();

    }

}
