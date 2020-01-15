package com.floragunn.signals.api;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

import java.io.IOException;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse.Result;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;

import com.floragunn.signals.actions.settings.get.GetSettingsAction;
import com.floragunn.signals.actions.settings.get.GetSettingsRequest;
import com.floragunn.signals.actions.settings.get.GetSettingsResponse;
import com.floragunn.signals.actions.settings.put.PutSettingsAction;
import com.floragunn.signals.actions.settings.put.PutSettingsRequest;
import com.floragunn.signals.actions.settings.put.PutSettingsResponse;

public class SettingsApiAction extends SignalsBaseRestHandler {
    public SettingsApiAction(final Settings settings, final RestController controller) {
        super(settings);
        controller.registerHandler(GET, "/_signals/settings", this);
        controller.registerHandler(GET, "/_signals/settings/{key}", this);
        controller.registerHandler(PUT, "/_signals/settings/{key}", this);
        controller.registerHandler(DELETE, "/_signals/settings/{key}", this);

    }

    @Override
    protected final RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {

        String key = request.param("key");

        return channel -> {
            handleApiRequest(key, channel, request, client);
        };
    }

    protected void handleApiRequest(String key, RestChannel channel, RestRequest request, Client client) throws IOException {

        switch (request.method()) {
        case GET:
            handleGet(key, channel, request, client);
            break;
        case PUT:
            handlePut(key, channel, request, client);
            break;
        case DELETE:
            handleDelete(key, channel, request, client);
            break;
        default:
            throw new IllegalArgumentException(request.method() + " not supported");
        }
    }

    protected void handleGet(String key, RestChannel channel, RestRequest request, Client client) throws IOException {
        
        client.execute(GetSettingsAction.INSTANCE, new GetSettingsRequest(key), new ActionListener<GetSettingsResponse>() {

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

    protected void handleDelete(String key, RestChannel channel, RestRequest request, Client client) throws IOException {
        client.execute(PutSettingsAction.INSTANCE, new PutSettingsRequest(key, null), new ActionListener<PutSettingsResponse>() {

            @Override
            public void onResponse(PutSettingsResponse response) {
                if (response.getResult() == Result.CREATED || response.getResult() == Result.UPDATED || response.getResult() == Result.DELETED) {

                    channel.sendResponse(new BytesRestResponse(response.getRestStatus(), convertToJson(channel, response, ToXContent.EMPTY_PARAMS)));
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

    protected void handlePut(String key, RestChannel channel, RestRequest request, Client client) throws IOException {

        client.execute(PutSettingsAction.INSTANCE, new PutSettingsRequest(key, request.content().utf8ToString()),
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
            throw ExceptionsHelper.convertToElastic(e);
        }
    }

    @Override
    public String getName() {
        return "Settings Action";
    }
}
