package com.floragunn.signals.actions.admin.start_stop;

import java.io.IOException;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

public class StartStopResponse extends ActionResponse {

    public StartStopResponse() {
    }

    public StartStopResponse(StreamInput in) throws IOException {

    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {

    }

}
