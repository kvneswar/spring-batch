package com.example.helloworld.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;

@Configuration
@EnableBatchProcessing
@Slf4j
public class JobConfig {

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    Tasklet tasklet = (contribution, chunkContext) -> {
            log.info("Step: {}, thread: {}", chunkContext.getStepContext().getStepName(), Thread.currentThread().getName());

            return RepeatStatus.FINISHED;
    };

    //@Bean
    public Step firstStep(){
        return stepBuilderFactory.get("step1")
                .tasklet(tasklet).build();
    }

    //@Bean
    public Step secondStep(){
        return stepBuilderFactory.get("step2")
                .tasklet(tasklet).build();
    }

    // To build the logical flow.
    //@Bean
    public Job jobBean(){
        return jobBuilderFactory.get("job2")
                .start(firstStep())
                    .on("COMPLETED").to(secondStep())
                .end()
                .build();
    }

    //@Bean
    public Job simpleJobBean(){
        return jobBuilderFactory.get("job3")
                .start(firstStep())
                .next(secondStep())
                .build();
    }

    //To fail the job
    //@Bean
    public Job jobFailBean(){
        return jobBuilderFactory.get("job2")
                .start(firstStep())
                .on("COMPLETED").to(secondStep())
                .on("COMPLETED").fail()
                .end()
                .build();
    }

    //To stop and restart
    //@Bean
    public Job restartJobBean(){
        return jobBuilderFactory.get("job3")
                .start(firstStep())
                .on("COMPLETED").to(secondStep())
                .on("COMPLETED").stopAndRestart(firstStep())
                .end()
                .build();
    }

    //@Bean
    public Flow foo(){
        FlowBuilder<Flow> flowFlowBuilder = new FlowBuilder<>("foo");

        return flowFlowBuilder.start(firstStep())
                .next(secondStep())
                .build();
    }

    //@Bean
    public Job flowFirstJob(){
        return jobBuilderFactory.get("flowFirstJob")
                .start(foo())
                .next(firstStep())
                .end()
                .build();
    }

    //@Bean
    public Job flowLastJob(){
        return jobBuilderFactory.get("flowLastJob")
                .start(firstStep())
                .on("COMPLETED").to(foo())
                .end()
                .build();
    }

    //@Bean
    public Job splitExample(){
        return jobBuilderFactory.get("splitExample")
                .start(foo())
                .split(new SimpleAsyncTaskExecutor())
                .add(foo())
                .end()
                .build();
    }



}
