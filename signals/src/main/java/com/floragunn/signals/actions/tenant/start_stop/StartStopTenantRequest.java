package com.floragunn.signals.actions.tenant.start_stop;

import java.io.IOException;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

public class StartStopTenantRequest extends ActionRequest {

    private boolean activate;

    public StartStopTenantRequest() {
        super();
    }

    public StartStopTenantRequest(boolean activate) {
        super();
        this.activate = activate;
    }

    public StartStopTenantRequest(StreamInput in) throws IOException {
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
