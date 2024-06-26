package com.floragunn.signals.actions.admin.start_stop;

import java.io.IOException;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

public class StartStopRequest extends ActionRequest {

    private boolean activate;

    public StartStopRequest() {
        super();
    }

    public StartStopRequest(boolean activate) {
        super();
        this.activate = activate;
    }

    public StartStopRequest(StreamInput in) throws IOException {
        super(in);
        this.activate = in.readBoolean();
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(activate);
    }

    @Override
    public ActionRequestValidationException validate() {

        return null;
    }

    public boolean isActivate() {
        return activate;
    }

    public void setActivate(boolean activate) {
        this.activate = activate;
    }

}
