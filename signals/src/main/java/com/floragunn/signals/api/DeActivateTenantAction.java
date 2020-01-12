package com.floragunn.signals.api;

import java.io.IOException;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequest.Method;
import org.elasticsearch.rest.RestStatus;

import com.floragunn.searchguard.filter.TenantAwareRestHandler;
import com.floragunn.signals.actions.tenant.start_stop.StartStopTenantAction;
import com.floragunn.signals.actions.tenant.start_stop.StartStopTenantRequest;
import com.floragunn.signals.actions.tenant.start_stop.StartStopTenantResponse;

public class DeActivateTenantAction extends SignalsBaseRestHandler implements TenantAwareRestHandler {

    public DeActivateTenantAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(Method.PUT, "/_signals/tenant/{tenant}/_active", this);
        controller.registerHandler(Method.DELETE, "/_signals/tenant/{tenant}/_active", this);
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
