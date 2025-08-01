package com.floragunn.searchguard.test.helper.cluster;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xcontent.ToXContentObject;

public class SimpleRestHandler<Request extends ActionRequest, Response extends ActionResponse & ToXContentObject> extends BaseRestHandler {

    private String name;
    private Route route;
    private Function<RestRequest, Request> requestFactory;
    private ActionType<Response> actionType;

    public SimpleRestHandler(Route route, ActionType<Response> actionType, Function<RestRequest, Request> requestFactory) {
        this.route = route;
        this.name = route.getMethod() + " " + route.getPath();
        this.actionType = actionType;
        this.requestFactory = requestFactory;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public List<Route> routes() {
        return Collections.singletonList(route);
    }

    @Override
    public RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {

        for (String param : request.params().keySet()) {
            request.param(param);
        }
        
        request.content();

        return channel -> client.execute(actionType, requestFactory.apply(request), new RestToXContentListener<Response>(channel));
    }

}
