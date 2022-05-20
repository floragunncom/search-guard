package com.floragunn.signals.actions.admin.start_stop;

import java.io.IOException;

import org.opensearch.action.ActionResponse;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;

public class StartStopResponse extends ActionResponse {

    public StartStopResponse() {
    }

    public StartStopResponse(StreamInput in) throws IOException {
        super(in);

    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {

    }

}
