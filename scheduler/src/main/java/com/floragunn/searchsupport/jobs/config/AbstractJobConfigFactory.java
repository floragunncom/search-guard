package com.floragunn.searchsupport.jobs.config;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.elasticsearch.common.bytes.BytesReference;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Trigger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.searchsupport.config.validation.ValidatingJsonParser;
import com.floragunn.searchsupport.jobs.config.schedule.DefaultScheduleFactory;
import com.floragunn.searchsupport.jobs.config.schedule.ScheduleFactory;
import com.jayway.jsonpath.TypeRef;

public abstract class AbstractJobConfigFactory<JobConfigType extends JobConfig> implements JobConfigFactory<JobConfigType> {
    protected String group = "main";
    protected Class<? extends Job> jobClass;
    protected ScheduleFactory<?> triggerFactory;

    protected final static TypeRef<Map<String, Object>> MAP_TYPE_REF = new TypeRef<Map<String, Object>>() {
    };

    public AbstractJobConfigFactory(Class<? extends Job> jobClass, ScheduleFactory<?> triggerFactory) {
        this.jobClass = jobClass;
        this.triggerFactory = triggerFactory != null ? triggerFactory : new DefaultScheduleFactory();
    }

    public AbstractJobConfigFactory(Class<? extends Job> jobClass) {
        this(jobClass, null);
    }

    @Override
    public JobConfigType createFromBytes(String id, BytesReference source, long version) throws ConfigValidationException {
        JsonNode jsonNode = ValidatingJsonParser.readTree(source.utf8ToString());

        return createFromJsonNode(id, jsonNode, version);
    }

    @Override
    public JobDetail createJobDetail(JobConfigType jobType) {
        JobBuilder jobBuilder = JobBuilder.newJob(jobType.getJobClass());

        jobBuilder.withIdentity(jobType.getJobKey());

        if (jobType.getJobDataMap() != null) {
            jobBuilder.setJobData(new JobDataMap(jobType.getJobDataMap()));
        }

        jobBuilder.withDescription(jobType.getDescription());
        jobBuilder.storeDurably(jobType.isDurable());
        return jobBuilder.build();
    }

    abstract protected JobConfigType createFromJsonNode(String id, JsonNode jsonNode, long version) throws ConfigValidationException;

    protected JobKey getJobKey(String id, JsonNode jsonNode) {
        return new JobKey(id, group);
    }

    protected String getDescription(JsonNode jsonNode) {
        if (jsonNode.hasNonNull("description")) {
            return jsonNode.get("description").asText();
        } else {
            return null;
        }
    }

    protected Map<String, Object> getJobDataMap(JsonNode jsonNode) {
        return Collections.emptyMap();
    }

    protected Boolean getDurability(JsonNode jsonNode) {
        return Boolean.TRUE;
    }

    protected List<Trigger> getTriggers(JobKey jobKey, JsonNode jsonNode) throws ConfigValidationException {

        JsonNode triggerNode = jsonNode.get("trigger");

        if (triggerNode instanceof ObjectNode) {
            try {
                return triggerFactory.create(jobKey, (ObjectNode) triggerNode).getTriggers();
            } catch (ConfigValidationException e) {
                ValidationErrors validationErrors = new ValidationErrors();
                validationErrors.add("trigger", e);
                validationErrors.throwExceptionForPresentErrors();
            }
        }

        return Collections.emptyList();
    }

    protected String getAuthToken(JsonNode jsonNode) {
        JsonNode authTokenNode = jsonNode.at("/meta/auth_token");
        if (authTokenNode != null && !authTokenNode.isMissingNode()) {
            return authTokenNode.asText();
        } else {
            return null;
        }
    }

    protected Class<? extends Job> getJobClass(JsonNode jsonNode) {
        return jobClass;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public Class<? extends Job> getJobClass() {
        return jobClass;
    }

    public void setJobClass(Class<? extends Job> jobClass) {
        this.jobClass = jobClass;
    }

}
