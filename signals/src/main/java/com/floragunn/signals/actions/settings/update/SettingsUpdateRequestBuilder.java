package com.floragunn.signals.actions.settings.update;

import org.elasticsearch.action.support.nodes.NodesOperationRequestBuilder;
import org.elasticsearch.client.ClusterAdminClient;
import org.elasticsearch.client.ElasticsearchClient;

public class SettingsUpdateRequestBuilder
        extends NodesOperationRequestBuilder<SettingsUpdateRequest, SettingsUpdateResponse, SettingsUpdateRequestBuilder> {
    public SettingsUpdateRequestBuilder(final ClusterAdminClient client) {
        this(client, SettingsUpdateAction.INSTANCE);
    }

    public SettingsUpdateRequestBuilder(final ElasticsearchClient client, final SettingsUpdateAction action) {
        super(client, action, new SettingsUpdateRequest());
    }
}
