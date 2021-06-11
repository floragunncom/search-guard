package com.floragunn.signals.actions.settings.update;

import java.io.IOException;

import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.common.io.stream.StreamInput;

public class SettingsUpdateRequest extends BaseNodesRequest<SettingsUpdateRequest> {

    public SettingsUpdateRequest() {
        super((String[]) null);
    }

    public SettingsUpdateRequest(StreamInput in) throws IOException {
        super(in);
    }
}
