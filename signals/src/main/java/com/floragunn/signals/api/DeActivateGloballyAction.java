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
import com.floragunn.signals.actions.admin.start_stop.StartStopAction;
import com.floragunn.signals.actions.admin.start_stop.StartStopRequest;
import com.floragunn.signals.actions.admin.start_stop.StartStopResponse;

public class DeActivateGloballyAction extends SignalsBaseRestHandler implements TenantAwareRestHandler {

    public DeActivateGloballyAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(Method.PUT, "/_signals/admin/_activate", this);
        controller.registerHandler(Method.PUT, "/_signals/admin/_deactivate", this);
    }

    @Override
    protected final RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {

        final boolean activate = request.path().endsWith("/_activate");

        return channel -> {

            client.execute(StartStopAction.INSTANCE, new StartStopRequest(activate), new ActionListener<StartStopResponse>() {

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
