package com.floragunn.signals.actions.watch.activate_deactivate;

import java.io.IOException;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;

public class DeActivateWatchRequest extends ActionRequest {

    private String watchId;
    private boolean activate;
    
    public DeActivateWatchRequest() {
        super();
    }

    public DeActivateWatchRequest(String watchId, boolean activate) {
        super();
        this.watchId = watchId;
        this.activate = activate;
    }
    public DeActivateWatchRequest(StreamInput in) throws IOException {
        super(in);
        this.watchId = in.readString();
        this.activate = in.readBoolean();
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(watchId);
        out.writeBoolean(activate);
    }

    @Override
    public ActionRequestValidationException validate() {
        if (watchId == null || watchId.length() == 0) {
            return new ActionRequestValidationException();
        }
        return null;
    }

    public String getWatchId() {
        return watchId;
    }

    public void setWatchId(String watchId) {
        this.watchId = watchId;
    }

    public boolean isActivate() {
        return activate;
    }

    public void setActivate(boolean activate) {
        this.activate = activate;
    }



}
