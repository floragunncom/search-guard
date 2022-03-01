package com.floragunn.signals.actions.account.config_update;

import org.opensearch.action.support.nodes.NodesOperationRequestBuilder;
import org.opensearch.client.ClusterAdminClient;
import org.opensearch.client.OpenSearchClient;

public class DestinationConfigUpdateRequestBuilder
        extends NodesOperationRequestBuilder<DestinationConfigUpdateRequest, DestinationConfigUpdateResponse, DestinationConfigUpdateRequestBuilder> {
    public DestinationConfigUpdateRequestBuilder(final ClusterAdminClient client) {
        this(client, DestinationConfigUpdateAction.INSTANCE);
    }

    public DestinationConfigUpdateRequestBuilder(final OpenSearchClient client, final DestinationConfigUpdateAction action) {
        super(client, action, new DestinationConfigUpdateRequest());
    }
}
