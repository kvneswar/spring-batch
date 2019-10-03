package com.example.demo;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JpaPagingItemReader;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.LineAggregator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;

import javax.persistence.EntityManagerFactory;

@Configuration
@EnableBatchProcessing
public class BatchConfig{

    @Autowired
    private EntityManagerFactory entityManagerFactory;

    @Bean
    public Job job1(JobBuilderFactory jobBuilderFactory,
                   StepBuilderFactory stepBuilderFactory,
                   ItemReader<User> flatFileItemReader,
                   ItemProcessor<User, User> userItemProcessor,
                   ItemWriter<User> dbWriter,

                   ItemReader<User> jpaPagingItemReader,
                   FileItemWriter fileItemWriter){
        Step step1 = stepBuilderFactory.get("etl-file-load")
                .<User, User>chunk(100)
                .reader(flatFileItemReader)
                .processor(userItemProcessor)
                .writer(dbWriter)
                .build();

        return jobBuilderFactory.get("etl-load")
                .incrementer(new RunIdIncrementer())
                .start(step1)
                .build();
    }

    @Bean
    public Job job2(JobBuilderFactory jobBuilderFactory,
                    StepBuilderFactory stepBuilderFactory,
                    ItemReader<User> flatFileItemReader,
                    ItemProcessor<User, User> userItemProcessor,
                    ItemWriter<User> dbWriter,

                    ItemReader<User> jpaPagingItemReader,
                    FileItemWriter fileItemWriter){

        Step step2 = stepBuilderFactory.get("etl-file-write")
                .<User, User>chunk(100)
                .reader(jpaPagingItemReader)
                .processor(u -> u)
                .writer(fileItemWriter)
                .build();

        return jobBuilderFactory.get("etl-write")
                .incrementer(new RunIdIncrementer())
                .start(step2)
                .build();
    }

    @Bean
    public FlatFileItemReader<User> flatFileItemReader(@Value("${input.file}") Resource resource){
        FlatFileItemReader<User> flatFileItemReader = new FlatFileItemReader<>();

        flatFileItemReader.setResource(resource);

        flatFileItemReader.setName("CSV Reader");
        flatFileItemReader.setLinesToSkip(1);
        flatFileItemReader.setLineMapper(userLineMapper());

        return flatFileItemReader;
    }

    public LineMapper<User> userLineMapper(){
        DefaultLineMapper<User> userDefaultLineMapper = new DefaultLineMapper<>();

        DelimitedLineTokenizer delimitedLineTokenizer = new DelimitedLineTokenizer();

        delimitedLineTokenizer.setDelimiter(",");
        delimitedLineTokenizer.setStrict(false);
        delimitedLineTokenizer.setNames(new String[]{"id", "name", "dept", "salary"});

        BeanWrapperFieldSetMapper<User> userBeanWrapperFieldSetMapper = new BeanWrapperFieldSetMapper<>();
        userBeanWrapperFieldSetMapper.setTargetType(User.class);

        userDefaultLineMapper.setLineTokenizer(delimitedLineTokenizer);
        userDefaultLineMapper.setFieldSetMapper(userBeanWrapperFieldSetMapper);

        return userDefaultLineMapper;
    }

    @Bean
    public JpaPagingItemReader<User> jpaPagingItemReader() throws Exception {
        JpaPagingItemReader<User> userJpaPagingItemReader = new JpaPagingItemReader<>();

        userJpaPagingItemReader.setEntityManagerFactory(entityManagerFactory);
        userJpaPagingItemReader.setQueryString("from User");
        userJpaPagingItemReader.setPageSize(5);
        userJpaPagingItemReader.afterPropertiesSet();
        userJpaPagingItemReader.setTransacted(false);

        return userJpaPagingItemReader;
    }

    @Bean
    public LineAggregator lineAggregator(){
        LineAggregator<User> userLineAggregator = new DelimitedLineAggregator<>();

        ((DelimitedLineAggregator)userLineAggregator).setDelimiter(",");

        return userLineAggregator;
    }

}
