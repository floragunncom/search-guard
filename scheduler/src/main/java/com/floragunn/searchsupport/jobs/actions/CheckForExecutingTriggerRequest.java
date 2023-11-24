package com.floragunn.searchsupport.jobs.actions;

import java.io.IOException;
import java.util.List;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

public class CheckForExecutingTriggerRequest extends BaseNodesRequest<CheckForExecutingTriggerRequest> {

    private String schedulerName;
    private List<String> triggerKeys;

    public CheckForExecutingTriggerRequest(StreamInput in) throws IOException {
        super(in);
        this.schedulerName = in.readString();
        this.triggerKeys = in.readStringCollectionAsList();
    }

    public CheckForExecutingTriggerRequest(String schedulerName, List<String> triggerKeys) {
        super(new String[] {});
        this.schedulerName = schedulerName;
        this.triggerKeys = triggerKeys;
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(schedulerName);
        out.writeStringCollection(triggerKeys);
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
