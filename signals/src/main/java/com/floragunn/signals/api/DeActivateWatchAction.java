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
import com.floragunn.signals.actions.watch.activate_deactivate.DeActivateWatchRequest;
import com.floragunn.signals.actions.watch.activate_deactivate.DeActivateWatchResponse;

public class DeActivateWatchAction extends SignalsBaseRestHandler implements TenantAwareRestHandler {

    public DeActivateWatchAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(Method.PUT, "/_signals/watch/{tenant}/{id}/_active", this);
        controller.registerHandler(Method.DELETE, "/_signals/watch/{tenant}/{id}/_active", this);
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
