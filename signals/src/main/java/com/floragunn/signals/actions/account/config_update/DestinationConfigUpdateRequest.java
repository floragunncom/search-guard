package com.floragunn.signals.actions.account.config_update;

import java.io.IOException;

import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.common.io.stream.StreamInput;

public class DestinationConfigUpdateRequest extends BaseNodesRequest<DestinationConfigUpdateRequest> {

    public DestinationConfigUpdateRequest() {
        super((String[]) null);
    }

    public DestinationConfigUpdateRequest(StreamInput in) throws IOException {
        super(in);
    }
}
