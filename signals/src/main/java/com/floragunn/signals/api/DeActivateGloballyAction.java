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
import com.floragunn.signals.actions.admin.start_stop.StartStopAction;
import com.floragunn.signals.actions.admin.start_stop.StartStopRequest;
import com.floragunn.signals.actions.admin.start_stop.StartStopResponse;
import com.google.common.collect.ImmutableList;

public class DeActivateGloballyAction extends SignalsBaseRestHandler implements TenantAwareRestHandler {

    public DeActivateGloballyAction(Settings settings, RestController controller) {
        super(settings);
    }

    @Override
    public List<Route> routes() {
        return ImmutableList.of(new Route(PUT, "/_signals/admin/_active"), new Route(DELETE, "/_signals/admin/_active"));
    }

    @Override
    protected final RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {

        final boolean active = request.method().equals(Method.PUT);

        return channel -> {

            client.execute(StartStopAction.INSTANCE, new StartStopRequest(active), new ActionListener<StartStopResponse>() {

                @Override
                public void onResponse(StartStopResponse response) {
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
        return "Activate/Deactivate Globally";
    }

}
