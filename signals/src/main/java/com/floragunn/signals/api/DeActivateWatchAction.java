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

import com.floragunn.searchguard.filter.TenantAwareRestHandler;
import com.floragunn.signals.actions.watch.activate_deactivate.DeActivateWatchRequest;
import com.floragunn.signals.actions.watch.activate_deactivate.DeActivateWatchResponse;
import com.google.common.collect.ImmutableList;

public class DeActivateWatchAction extends SignalsBaseRestHandler implements TenantAwareRestHandler {

    public DeActivateWatchAction(Settings settings, RestController controller) {
        super(settings);
    }

    @Override
    public List<Route> routes() {
        return ImmutableList.of(new Route(PUT, "/_signals/watch/{tenant}/{id}/_active"), new Route(DELETE, "/_signals/watch/{tenant}/{id}/_active"));
    }

    @Override
    protected final RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {

        final String id = request.param("id");

        final boolean active = request.method().equals(Method.PUT);

        return channel -> {

            client.execute(com.floragunn.signals.actions.watch.activate_deactivate.DeActivateWatchAction.INSTANCE,
                    new DeActivateWatchRequest(id, active), new ActionListener<DeActivateWatchResponse>() {

                        @Override
                        public void onResponse(DeActivateWatchResponse response) {
                            if (response.getRestStatus() == RestStatus.OK) {
                                response(channel, RestStatus.OK);
                            } else {
                                errorResponse(channel, response.getRestStatus(), response.getMessage());
                            }
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
        return "Activate/Deactivate Watch";
    }

}
