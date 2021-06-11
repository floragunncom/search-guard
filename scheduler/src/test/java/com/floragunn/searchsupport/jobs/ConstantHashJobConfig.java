package com.floragunn.searchsupport.jobs;

import org.quartz.Job;

import com.fasterxml.jackson.databind.JsonNode;
import com.floragunn.searchsupport.config.validation.ConfigValidationException;
import com.floragunn.searchsupport.jobs.config.DefaultJobConfig;
import com.floragunn.searchsupport.jobs.config.DefaultJobConfigFactory;

public class ConstantHashJobConfig extends DefaultJobConfig {

    private int hashCode;

    public ConstantHashJobConfig(Class<? extends Job> jobClass) {
        super(jobClass);
    }

    public int hashCode() {
        return hashCode;
    }

    public static class Factory extends DefaultJobConfigFactory {

        public Factory(Class<? extends Job> jobClass) {
            super(jobClass);
        }

        @Override
        protected DefaultJobConfig createObject(String id, JsonNode jsonNode) {
            return new ConstantHashJobConfig(getJobClass(jsonNode));
        }

        @Override
        protected DefaultJobConfig createFromJsonNode(String id, JsonNode jsonNode, long version) throws ConfigValidationException {
            ConstantHashJobConfig result = (ConstantHashJobConfig) super.createFromJsonNode(id, jsonNode, version);

            if (jsonNode.hasNonNull("hash")) {
                result.hashCode = jsonNode.get("hash").asInt();
            } else {
                result.hashCode = result.getJobKey().hashCode();
            }

            return result;
        }
    }
}
