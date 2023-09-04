package com.floragunn.signals.api;

import static org.elasticsearch.rest.RestRequest.Method.GET;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;

import com.floragunn.signals.actions.watch.state.get.GetWatchStateAction;
import com.floragunn.signals.actions.watch.state.get.GetWatchStateRequest;
import com.floragunn.signals.actions.watch.state.get.GetWatchStateResponse;
import com.google.common.collect.ImmutableList;

public class WatchStateApiAction extends SignalsBaseRestHandler {

    public WatchStateApiAction(Settings settings, RestController controller) {
        super(settings);
    }

    @Override
    public List<Route> routes() {
        return ImmutableList.of(new Route(GET, "/_signals/watch/{tenant}/{id}/_state"));
    }

    @Override
    protected final RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {

        final String watchId = request.param("id");

        //we need to consume the tenant param here because
        //if not ES 8 throws an exception
        request.param("tenant");

        return channel -> {

            // TODO action listener which handles exceptions within onResponse and onFailure
            client.execute(GetWatchStateAction.INSTANCE, new GetWatchStateRequest(Collections.singletonList(watchId)),
                    new ActionListener<GetWatchStateResponse>() {

                        @Override
                        public void onResponse(GetWatchStateResponse response) {
                            BytesReference statusDoc = response.getWatchToStatusMap().get(watchId);

                            if (statusDoc != null) {
                                channel.sendResponse(new RestResponse(RestStatus.OK, "application/json", statusDoc));
                            } else {
                                errorResponse(channel, RestStatus.NOT_FOUND, "No such watch: " + watchId);
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
        return "Watch State";
    }

}
