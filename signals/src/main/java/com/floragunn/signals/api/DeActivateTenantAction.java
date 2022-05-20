package com.floragunn.signals.api;

import static org.opensearch.rest.RestRequest.Method.DELETE;
import static org.opensearch.rest.RestRequest.Method.PUT;

import java.io.IOException;
import java.util.List;

import org.opensearch.action.ActionListener;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.settings.Settings;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestRequest.Method;
import org.opensearch.rest.RestStatus;

import com.floragunn.searchguard.authc.rest.TenantAwareRestHandler;
import com.floragunn.signals.actions.tenant.start_stop.StartStopTenantAction;
import com.floragunn.signals.actions.tenant.start_stop.StartStopTenantRequest;
import com.floragunn.signals.actions.tenant.start_stop.StartStopTenantResponse;
import com.google.common.collect.ImmutableList;

public class DeActivateTenantAction extends SignalsBaseRestHandler implements TenantAwareRestHandler {

    public DeActivateTenantAction(Settings settings, RestController controller) {
        super(settings);
    }

    @Override
    public List<Route> routes() {
        return ImmutableList.of(new Route(PUT, "/_signals/tenant/{tenant}/_active"), new Route(DELETE, "/_signals/tenant/{tenant}/_active"));
    }

    @Override
    protected final RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {

        final boolean active = request.method().equals(Method.PUT);

        return channel -> {

            client.execute(StartStopTenantAction.INSTANCE, new StartStopTenantRequest(active), new ActionListener<StartStopTenantResponse>() {

                @Override
                public void onResponse(StartStopTenantResponse response) {
                    response(channel, RestStatus.OK);
                }

                @Override
                public void onFailure(Exception e) {
                    errorResponse(channel, e);
                }
            });

        };

    }

    @Override
    public String getName() {
        return "Activate/Deactivate Tenant";
    }

}
