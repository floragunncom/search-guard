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
import com.floragunn.signals.actions.watch.ack.AckWatchAction;
import com.floragunn.signals.actions.watch.ack.AckWatchRequest;
import com.floragunn.signals.actions.watch.ack.AckWatchResponse;
import com.google.common.collect.ImmutableList;

public class AckWatchApiAction extends SignalsBaseRestHandler implements TenantAwareRestHandler {

    public AckWatchApiAction(Settings settings, RestController controller) {
        super(settings);
    }

    @Override
    public List<Route> routes() {
        return ImmutableList.of(new Route(PUT, "/_signals/watch/{tenant}/{id}/_ack"), new Route(PUT, "/_signals/watch/{tenant}/{id}/_ack/{actionId}"),
                new Route(DELETE, "/_signals/watch/{tenant}/{id}/_ack"), new Route(DELETE, "/_signals/watch/{tenant}/{id}/_ack/{actionId}"));
    }

    @Override
    protected final RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {

        final String watchId = request.param("id");
        final String actionId = request.param("actionId");

        return channel -> {

            client.execute(AckWatchAction.INSTANCE, new AckWatchRequest(watchId, actionId, request.getHttpRequest().method() == Method.PUT),
                    new ActionListener<AckWatchResponse>() {

                        @Override
                        public void onResponse(AckWatchResponse response) {
                            if (response.getStatus() == AckWatchResponse.Status.SUCCESS) {
                                response(channel, RestStatus.OK);
                            } else if (response.getStatus() == AckWatchResponse.Status.NO_SUCH_WATCH) {
                                errorResponse(channel, RestStatus.NOT_FOUND, response.getStatusMessage());
                            } else if (response.getStatus() == AckWatchResponse.Status.ILLEGAL_STATE) {
                                errorResponse(channel, RestStatus.PRECONDITION_FAILED, response.getStatusMessage());
                            } else {
                                errorResponse(channel, RestStatus.INTERNAL_SERVER_ERROR, response.getStatusMessage());
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
        return "Ack Watch";
    }

}
