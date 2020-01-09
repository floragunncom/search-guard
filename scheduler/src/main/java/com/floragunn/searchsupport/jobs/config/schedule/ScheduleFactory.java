package com.floragunn.searchsupport.jobs.config.schedule;

import org.quartz.JobKey;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.floragunn.searchsupport.jobs.config.validation.ConfigValidationException;

public interface ScheduleFactory<X extends Schedule> {
    public X create(JobKey jobKey, ObjectNode objectNode) throws ConfigValidationException;
}
