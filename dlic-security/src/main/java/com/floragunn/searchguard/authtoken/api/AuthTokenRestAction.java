package com.floragunn.searchguard.authtoken.api;

import static org.elasticsearch.rest.RestRequest.Method.POST;

import java.io.IOException;
import java.util.List;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import com.floragunn.searchsupport.client.rest.Responses;
import com.floragunn.searchsupport.config.validation.ConfigValidationException;
import com.google.common.collect.ImmutableList;

public class AuthTokenRestAction extends BaseRestHandler {
    public AuthTokenRestAction() {
        super();
    }

    @Override
    public List<Route> routes() {
        return ImmutableList.of(new Route(POST, "/_searchguard/authtoken"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        return (RestChannel channel) -> {

            try {
                CreateAuthTokenRequest authTokenRequest = CreateAuthTokenRequest.parse(request.requiredContent(), request.getXContentType());

                client.execute(CreateAuthTokenAction.INSTANCE, authTokenRequest, new RestToXContentListener<CreateAuthTokenResponse>(channel));
            } catch (ConfigValidationException e) {
                Responses.sendError(channel, e);
            }
        };

    }

    @Override
    public String getName() {
        return "Search Guard Auth Token";
    }
}
