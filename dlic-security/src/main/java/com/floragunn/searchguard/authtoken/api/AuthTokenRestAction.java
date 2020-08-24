package com.floragunn.searchguard.authtoken.api;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;
import static org.elasticsearch.rest.RestRequest.Method.POST;

import java.io.IOException;
import java.util.List;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import com.floragunn.searchsupport.client.rest.Responses;
import com.google.common.collect.ImmutableList;

public class AuthTokenRestAction extends BaseRestHandler {
    public AuthTokenRestAction() {
        super();
    }

    @Override
    public List<Route> routes() {
        return ImmutableList.of(new Route(POST, "/_searchguard/authtoken"), new Route(DELETE, "/_searchguard/authtoken/{id}"));

    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {

        return (RestChannel channel) -> {
            if (request.method() == POST) {
                handlePost(request, client, channel);
            } else if (request.method() == DELETE) {
                handleDelete(request, client, channel);
            }
        };

    }

    private void handlePost(RestRequest request, NodeClient client, RestChannel channel) {

        try {
            CreateAuthTokenRequest authTokenRequest = CreateAuthTokenRequest.parse(request.requiredContent(), request.getXContentType());

            client.execute(CreateAuthTokenAction.INSTANCE, authTokenRequest, new RestToXContentListener<CreateAuthTokenResponse>(channel));
        } catch (Exception e) {
            Responses.sendError(channel, e);
        }
    }

    private void handleDelete(RestRequest request, NodeClient client, RestChannel channel) {

        try {
            client.execute(RevokeAuthTokenAction.INSTANCE, new RevokeAuthTokenRequest(request.param("id")),
                    new RestToXContentListener<RevokeAuthTokenResponse>(channel));
        } catch (Exception e) {
            Responses.sendError(channel, e);
        }
    }

    @Override
    public String getName() {
        return "Search Guard Auth Token";
    }
}
