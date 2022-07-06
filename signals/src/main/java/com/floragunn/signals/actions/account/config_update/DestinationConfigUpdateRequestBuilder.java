package com.floragunn.signals.actions.account.config_update;

import org.elasticsearch.action.support.nodes.NodesOperationRequestBuilder;
import org.elasticsearch.client.internal.ClusterAdminClient;
import org.elasticsearch.client.internal.ElasticsearchClient;

public class DestinationConfigUpdateRequestBuilder
        extends NodesOperationRequestBuilder<DestinationConfigUpdateRequest, DestinationConfigUpdateResponse, DestinationConfigUpdateRequestBuilder> {
    public DestinationConfigUpdateRequestBuilder(final ClusterAdminClient client) {
        this(client, DestinationConfigUpdateAction.INSTANCE);
    }

    public DestinationConfigUpdateRequestBuilder(final ElasticsearchClient client, final DestinationConfigUpdateAction action) {
        super(client, action, new DestinationConfigUpdateRequest());
    }
}
