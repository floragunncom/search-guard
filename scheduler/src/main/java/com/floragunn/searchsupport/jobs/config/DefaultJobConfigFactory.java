package com.floragunn.searchsupport.jobs.config;

import org.quartz.Job;
import org.quartz.JobKey;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.validation.ConfigValidationException;

public class DefaultJobConfigFactory extends AbstractJobConfigFactory<DefaultJobConfig> implements JobConfigFactory<DefaultJobConfig> {

    public DefaultJobConfigFactory(Class<? extends Job> jobClass) {
        super(jobClass);
    }

    protected DefaultJobConfig createObject(String id, DocNode jsonNode) {
        return new DefaultJobConfig(getJobClass(jsonNode));
    }

    protected DefaultJobConfig createFromJsonNode(String id, DocNode jsonNode, long version) throws ConfigValidationException {
        DefaultJobConfig result = createObject(id, jsonNode);
        JobKey jobKey = getJobKey(id, jsonNode);

        result.setJobKey(jobKey);

        result.setDescription(getDescription(jsonNode));

        Boolean durability = getDurability(jsonNode);

        if (durability != null) {
            result.setDurable(durability);
        }

        result.setJobDataMap(getJobDataMap(jsonNode));
        result.setTriggers(getTriggers(jobKey, jsonNode));
        result.setVersion(version);
        result.setAuthToken(getAuthToken(jsonNode));

        return result;
    }
}
