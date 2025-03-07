package com.floragunn.signals.api;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

import java.io.IOException;
import java.util.List;

import com.floragunn.searchsupport.rest.ProtectingContentRequestWrapper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse.Result;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.XContentType;

import com.floragunn.signals.actions.watch.delete.DeleteWatchAction;
import com.floragunn.signals.actions.watch.delete.DeleteWatchRequest;
import com.floragunn.signals.actions.watch.delete.DeleteWatchResponse;
import com.floragunn.signals.actions.watch.get.GetWatchAction;
import com.floragunn.signals.actions.watch.get.GetWatchRequest;
import com.floragunn.signals.actions.watch.get.GetWatchResponse;
import com.floragunn.signals.actions.watch.put.PutWatchAction;
import com.floragunn.signals.actions.watch.put.PutWatchRequest;
import com.floragunn.signals.actions.watch.put.PutWatchResponse;
import com.floragunn.signals.watch.Watch;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;

public class WatchApiAction extends SignalsTenantAwareRestHandler {

    public WatchApiAction(final Settings settings) {
        super(settings);
    }

    @Override
    public List<Route> routes() {
        return ImmutableList.of(new Route(GET, "/_signals/watch/{tenant}/{id}"), new Route(PUT, "/_signals/watch/{tenant}/{id}"),
                new Route(DELETE, "/_signals/watch/{tenant}/{id}"));
    }

    @Override
    protected final RestChannelConsumer getRestChannelConsumer(RestRequest request, NodeClient client) throws IOException {

        String id = request.param("id");

        //we need to consume the tenant param here because
        //if not ES 8 throws an exception
        String t = request.param("tenant");


        if (Strings.isNullOrEmpty(id)) {
            return channel -> errorResponse(channel, RestStatus.BAD_REQUEST, "No id specified");
        }

        switch (request.method()) {
        case GET:
            return handleGet(id, request, client);
        case PUT:
            return handlePut(id, request, client);
        case DELETE:
            return handleDelete(id, request, client);
        default:
            throw new IllegalArgumentException(request.method() + " not supported");
        }
    }

    protected RestChannelConsumer handleGet(String id, RestRequest request, Client client) throws IOException {

        return channel -> client.execute(GetWatchAction.INSTANCE, new GetWatchRequest(id), new ActionListener<GetWatchResponse>() {

            @Override
            public void onResponse(GetWatchResponse response) {
                if (response.isExists()) {
                    channel.sendResponse(new RestResponse(RestStatus.OK, convertToJson(channel, response, Watch.WITHOUT_AUTH_TOKEN)));
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

    protected RestChannelConsumer handleDelete(String id, RestRequest request, Client client) throws IOException {

        return channel -> client.execute(DeleteWatchAction.INSTANCE, new DeleteWatchRequest(id), new ActionListener<DeleteWatchResponse>() {

            @Override
            public void onResponse(DeleteWatchResponse response) {
                if (response.getResult() == Result.DELETED) {
                    channel.sendResponse(new RestResponse(RestStatus.OK, convertToJson(channel, response, Watch.WITHOUT_AUTH_TOKEN)));
                } else {
                    errorResponse(channel, response.getRestStatus(), response.getMessage());
                }
            }

            @Override
            public void onFailure(Exception e) {
                errorResponse(channel, e);
            }
        });

    }

    protected RestChannelConsumer handlePut(String id, RestRequest request, Client client) throws IOException {



        if (request.getXContentType() != XContentType.JSON && request.getXContentType() != XContentType.VND_JSON) {
            return channel -> errorResponse(channel, RestStatus.UNSUPPORTED_MEDIA_TYPE, "Watches must be of content type application/json");
        }

        ProtectingContentRequestWrapper protectedRequest = new ProtectingContentRequestWrapper(request);
        return channel -> {
            ActionListener<PutWatchResponse> listener = new ActionListener<>() {

                @Override
                public void onResponse(PutWatchResponse response) {
                    if (response.getResult() == Result.CREATED || response.getResult() == Result.UPDATED) {

                        channel.sendResponse(
                                new RestResponse(response.getRestStatus(), convertToJson(channel, response, Watch.WITHOUT_AUTH_TOKEN)));
                    } else {
                        errorResponse(channel, response.getRestStatus(), response.getMessage(), response.getDetailJsonDocument());
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    errorResponse(channel, e);
                }
            };
            client.execute(PutWatchAction.INSTANCE, new PutWatchRequest(id, protectedRequest.content(), XContentType.JSON), protectedRequest.releaseAfter(listener));
        };

    }

    @Override
    public String getName() {
        return "Watch Action";
    }

}
