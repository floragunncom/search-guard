package com.floragunn.signals.actions.settings.update;

import org.elasticsearch.action.support.nodes.BaseNodesRequest;

public class SettingsUpdateRequest extends BaseNodesRequest<SettingsUpdateRequest> {

    public SettingsUpdateRequest() {
        super((String[]) null);
    }
}
