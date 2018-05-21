package com.pluralsight.springbatch.patientbatchloader.config;

import org.apache.commons.lang3.StringUtils;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.JobParametersValidator;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.support.JobRegistryBeanPostProcessor;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

@Configuration
public class BatchJobConfiguration {

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private ApplicationProperties applicationProperties;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Bean
    JobRegistryBeanPostProcessor jobRegistryBeanPostProcessor(JobRegistry jobRegistry) {
        JobRegistryBeanPostProcessor postProcessor = new JobRegistryBeanPostProcessor();
        postProcessor.setJobRegistry(jobRegistry);
        return postProcessor;
    }

    @Bean
    public Job job(Step step) throws Exception {
        return jobBuilderFactory
            .get(Constants.JOB_NAME)
            .validator(validator())
            .start(step)
            .build();


    }

    @Bean
    public Step step() throws Exception {
        return this.stepBuilderFactory
            .get(Constants.STEP_NAME)
            .tasklet((contribution, chunkContext) -> {
                System.err.println("Hello World!");
                return RepeatStatus.FINISHED;
            })
            .build();
    }

    private JobParametersValidator validator() {

        return parameters -> {
            String fileName = parameters.getString(Constants.JOB_PARAM_FILE_NAME);

            if (StringUtils.isBlank(fileName)) {
                throw new JobParametersInvalidException(
                    "The patient-batch-loader fileName parameter is required.");
            }

            try {
                Path file = Paths.get(applicationProperties.getBatch().getInputPath() +
                    File.separator + fileName);

                if (Files.notExists(file) || !Files.isReadable(file)) {

                    throw new Exception("File did not exist or was not readable.");

                }
            } catch (Exception e) {
                throw new JobParametersInvalidException(
                    "The input path + patient-batch-loader.fileName parameter needs to be a valid file location.");
            }
        };

    }
}
