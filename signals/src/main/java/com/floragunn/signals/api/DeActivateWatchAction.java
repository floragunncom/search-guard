package com.floragunn.signals.api;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

import java.io.IOException;
import java.util.List;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequest.Method;
import org.elasticsearch.rest.RestStatus;

import com.floragunn.signals.actions.watch.activate_deactivate.DeActivateWatchRequest;
import com.floragunn.signals.actions.watch.activate_deactivate.DeActivateWatchResponse;
import com.google.common.collect.ImmutableList;

public class DeActivateWatchAction extends SignalsTenantAwareRestHandler {

    public DeActivateWatchAction(Settings settings, RestController controller) {
        super(settings);
    }

    @Override
    public List<Route> routes() {
        return ImmutableList.of(new Route(PUT, "/_signals/watch/{tenant}/{id}/_active"), new Route(DELETE, "/_signals/watch/{tenant}/{id}/_active"));
    }

    @Override
    protected final RestChannelConsumer getRestChannelConsumer(RestRequest request, NodeClient client) throws IOException {

        final String id = request.param("id");

        //we need to consume the tenant param here because
        //if not ES 8 throws an exception
        request.param("tenant");

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
