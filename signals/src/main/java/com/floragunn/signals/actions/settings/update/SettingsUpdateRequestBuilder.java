package com.floragunn.signals.actions.settings.update;

import org.opensearch.action.support.nodes.NodesOperationRequestBuilder;
import org.opensearch.client.ClusterAdminClient;
import org.opensearch.client.OpenSearchClient;

public class SettingsUpdateRequestBuilder
        extends NodesOperationRequestBuilder<SettingsUpdateRequest, SettingsUpdateResponse, SettingsUpdateRequestBuilder> {
    public SettingsUpdateRequestBuilder(final ClusterAdminClient client) {
        this(client, SettingsUpdateAction.INSTANCE);
    }

    public SettingsUpdateRequestBuilder(final OpenSearchClient client, final SettingsUpdateAction action) {
        super(client, action, new SettingsUpdateRequest());
    }
}
