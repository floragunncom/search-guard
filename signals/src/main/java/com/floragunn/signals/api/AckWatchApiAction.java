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
import com.floragunn.signals.actions.watch.ack.AckWatchAction;
import com.floragunn.signals.actions.watch.ack.AckWatchRequest;
import com.floragunn.signals.actions.watch.ack.AckWatchResponse;

public class AckWatchApiAction extends SignalsBaseRestHandler implements TenantAwareRestHandler {

    public AckWatchApiAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(Method.PUT, "/_signals/watch/{tenant}/{id}/_ack", this);
        controller.registerHandler(Method.PUT, "/_signals/watch/{tenant}/{id}/_ack/{actionId}", this);
        controller.registerHandler(Method.DELETE, "/_signals/watch/{tenant}/{id}/_ack", this);
        controller.registerHandler(Method.DELETE, "/_signals/watch/{tenant}/{id}/_ack/{actionId}", this);
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
