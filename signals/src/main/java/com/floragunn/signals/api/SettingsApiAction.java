package com.floragunn.signals.api;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

import java.io.IOException;
import java.util.List;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse.Result;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import com.floragunn.signals.actions.settings.get.GetSettingsAction;
import com.floragunn.signals.actions.settings.get.GetSettingsRequest;
import com.floragunn.signals.actions.settings.get.GetSettingsResponse;
import com.floragunn.signals.actions.settings.put.PutSettingsAction;
import com.floragunn.signals.actions.settings.put.PutSettingsRequest;
import com.floragunn.signals.actions.settings.put.PutSettingsResponse;
import com.google.common.collect.ImmutableList;

public class SettingsApiAction extends SignalsBaseRestHandler {

    public SettingsApiAction(final Settings settings, final RestController controller) {
        super(settings);
    }

    @Override
    public List<Route> routes() {
        return ImmutableList.of(new Route(GET, "/_signals/settings"), new Route(GET, "/_signals/settings/{key}"),
                new Route(PUT, "/_signals/settings/{key}"), new Route(DELETE, "/_signals/settings/{key}"));
    }

    @Override
    protected final RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {

        String key = request.param("key");

        switch (request.method()) {
        case GET:
            return handleGet(key, request, client);
        case PUT:
            return handlePut(key, request, client);
        case DELETE:
            return handleDelete(key, request, client);
        default:
            throw new IllegalArgumentException(request.method() + " not supported");
        }

    }

    protected RestChannelConsumer handleGet(String key, RestRequest request, Client client) throws IOException {

        return channel -> client.execute(GetSettingsAction.INSTANCE, new GetSettingsRequest(key, jsonRequested(request)),
                new ActionListener<GetSettingsResponse>() {

                    @Override
                    public void onResponse(GetSettingsResponse response) {
                        if (response.getStatus() == GetSettingsResponse.Status.OK) {
                            channel.sendResponse(new BytesRestResponse(RestStatus.OK, response.getContentType(), response.getResult()));
                        } else {
                            errorResponse(channel, RestStatus.NOT_FOUND, "Not found");
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        errorResponse(channel, e);
                    }
                });
    }

    protected RestChannelConsumer handleDelete(String key, RestRequest request, Client client) throws IOException {
        return channel -> client.execute(PutSettingsAction.INSTANCE, new PutSettingsRequest(key, null, false),
                new ActionListener<PutSettingsResponse>() {

                    @Override
                    public void onResponse(PutSettingsResponse response) {
                        if (response.getResult() == Result.CREATED || response.getResult() == Result.UPDATED
                                || response.getResult() == Result.DELETED) {

                            channel.sendResponse(
                                    new BytesRestResponse(response.getRestStatus(), convertToJson(channel, response, ToXContent.EMPTY_PARAMS)));
                        } else {
                            errorResponse(channel, response.getRestStatus(), response.getMessage(), response.getDetailJsonDocument());
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        errorResponse(channel, e);
                    }
                });
    }

    protected RestChannelConsumer handlePut(String key, RestRequest request, Client client) throws IOException {

        String content = request.content().utf8ToString();
        boolean contentIsJson = request.getXContentType() == XContentType.JSON || request.getXContentType() == XContentType.VND_JSON;

        return channel -> client.execute(PutSettingsAction.INSTANCE, new PutSettingsRequest(key, content, contentIsJson),
                new ActionListener<PutSettingsResponse>() {

                    @Override
                    public void onResponse(PutSettingsResponse response) {
                        if (response.getResult() == Result.CREATED || response.getResult() == Result.UPDATED) {

                            channel.sendResponse(
                                    new BytesRestResponse(response.getRestStatus(), convertToJson(channel, response, ToXContent.EMPTY_PARAMS)));
                        } else {
                            errorResponse(channel, response.getRestStatus(), response.getMessage(), response.getDetailJsonDocument());
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        errorResponse(channel, e);
                    }
                });

    }

    protected static XContentBuilder convertToJson(RestChannel channel, ToXContent toXContent, ToXContent.Params params) {
        try {
            XContentBuilder builder = channel.newBuilder();
            toXContent.toXContent(builder, params);
            return builder;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean jsonRequested(RestRequest request) {
        String accept = request.header("Accept");

        if (accept == null) {
            return true;
        }

        String[] array = accept.split("\\s*,\\s*");

        for (String value : array) {
            if (value.startsWith("text/plain")) {
                return false;
            }
        }

        return true;
    }

    @Override
    public String getName() {
        return "Settings Action";
    }
}
