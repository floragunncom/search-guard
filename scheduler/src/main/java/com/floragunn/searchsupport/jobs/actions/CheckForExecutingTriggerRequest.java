package com.floragunn.searchsupport.jobs.actions;

import java.util.List;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;

public class CheckForExecutingTriggerRequest extends BaseNodesRequest<CheckForExecutingTriggerRequest> {

    private String schedulerName;
    private List<String> triggerKeys;

    public CheckForExecutingTriggerRequest(String schedulerName, List<String> triggerKeys) {
        super(new String[] {});
        this.schedulerName = schedulerName;
        this.triggerKeys = triggerKeys;
    }

    @Override
    public ActionRequestValidationException validate() {
        if (schedulerName == null || schedulerName.length() == 0) {
            return new ActionRequestValidationException();
        }
        return null;
    }

    public String getSchedulerName() {
        return schedulerName;
    }

    public void setSchedulerName(String schedulerName) {
        this.schedulerName = schedulerName;
    }

    public List<String> getTriggerKeys() {
        return triggerKeys;
    }

    public void setTriggerKeys(List<String> triggerKeys) {
        this.triggerKeys = triggerKeys;
    }

}
