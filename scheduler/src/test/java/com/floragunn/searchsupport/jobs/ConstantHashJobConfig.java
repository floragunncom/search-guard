package com.floragunn.searchsupport.jobs;

import org.quartz.Job;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.validation.ConfigValidationException;
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
        protected DefaultJobConfig createObject(String id, DocNode jsonNode) {
            return new ConstantHashJobConfig(getJobClass(jsonNode));
        }

        @Override
        protected DefaultJobConfig createFromJsonNode(String id, DocNode jsonNode, long version) throws ConfigValidationException {
            ConstantHashJobConfig result = (ConstantHashJobConfig) super.createFromJsonNode(id, jsonNode, version);

            if (jsonNode.hasNonNull("hash")) {
                result.hashCode = Integer.parseInt(jsonNode.getAsString("hash"));
            } else {
                result.hashCode = result.getJobKey().hashCode();
            }

            return result;
        }
    }
}
