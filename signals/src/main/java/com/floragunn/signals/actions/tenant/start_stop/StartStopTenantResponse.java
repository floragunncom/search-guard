package com.floragunn.signals.actions.tenant.start_stop;

import java.io.IOException;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

public class StartStopTenantResponse extends ActionResponse {

    public StartStopTenantResponse() {
    }

    public StartStopTenantResponse(StreamInput in) throws IOException {

    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {

    }

}
