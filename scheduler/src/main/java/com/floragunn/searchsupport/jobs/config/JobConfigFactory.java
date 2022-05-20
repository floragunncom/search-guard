package com.floragunn.searchsupport.jobs.config;

import java.io.IOException;

import org.opensearch.common.bytes.BytesReference;
import org.quartz.JobDetail;

import com.floragunn.codova.validation.ConfigValidationException;

public interface JobConfigFactory<JobConfigType extends JobConfig> {
    JobConfigType createFromBytes(String id, BytesReference source, long version) throws ConfigValidationException, IOException;

    JobDetail createJobDetail(JobConfigType jobType);
}
