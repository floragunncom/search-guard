package com.floragunn.signals.api;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

import java.io.IOException;
import java.util.List;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse.Result;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;

import com.floragunn.searchguard.filter.TenantAwareRestHandler;
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

public class WatchApiAction extends SignalsBaseRestHandler implements TenantAwareRestHandler {

    public WatchApiAction(final Settings settings, final RestController controller, final ThreadPool threadPool) {
        super(settings);
    }

    @Override
    public List<Route> routes() {
        return ImmutableList.of(new Route(GET, "/_signals/watch/{tenant}/{id}"), new Route(PUT, "/_signals/watch/{tenant}/{id}"),
                new Route(DELETE, "/_signals/watch/{tenant}/{id}"));
    }

    @Override
    protected final RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {

        String id = request.param("id");

        if (Strings.isNullOrEmpty(id)) {
            return channel -> {
                errorResponse(channel, RestStatus.BAD_REQUEST, "No id specified");
            };
        }

        return channel -> {
            handleApiRequest(id, channel, request, client);
        };
    }

    protected void handleApiRequest(String id, RestChannel channel, RestRequest request, Client client) throws IOException {

        switch (request.method()) {
        case GET:
            handleGet(id, channel, request, client);
            break;
        case PUT:
            handlePut(id, channel, request, client);
            break;
        case DELETE:
            handleDelete(id, channel, request, client);
            break;
        case POST:
            handlePost(id, channel, request, client);
            break;
        default:
            throw new IllegalArgumentException(request.method() + " not supported");
        }
    }

    protected void handleGet(String id, RestChannel channel, RestRequest request, Client client) throws IOException {

        client.execute(GetWatchAction.INSTANCE, new GetWatchRequest(id), new ActionListener<GetWatchResponse>() {

            @Override
            public void onResponse(GetWatchResponse response) {
                if (response.isExists()) {
                    channel.sendResponse(new BytesRestResponse(RestStatus.OK, convertToJson(channel, response, Watch.WITHOUT_AUTH_TOKEN)));
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

    protected void handleDelete(String id, RestChannel channel, RestRequest request, Client client) throws IOException {

        client.execute(DeleteWatchAction.INSTANCE, new DeleteWatchRequest(id), new ActionListener<DeleteWatchResponse>() {

            @Override
            public void onResponse(DeleteWatchResponse response) {
                if (response.getResult() == Result.DELETED) {
                    channel.sendResponse(new BytesRestResponse(RestStatus.OK, convertToJson(channel, response, Watch.WITHOUT_AUTH_TOKEN)));
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

    protected void handlePut(String id, RestChannel channel, RestRequest request, Client client) throws IOException {

        if (request.getXContentType() != XContentType.JSON) {
            errorResponse(channel, RestStatus.UNSUPPORTED_MEDIA_TYPE, "Watches must be of content type application/json");
            return;
        }

        client.execute(PutWatchAction.INSTANCE, new PutWatchRequest(id, request.content(), XContentType.JSON),
                new ActionListener<PutWatchResponse>() {

                    @Override
                    public void onResponse(PutWatchResponse response) {
                        if (response.getResult() == Result.CREATED || response.getResult() == Result.UPDATED) {

                            channel.sendResponse(
                                    new BytesRestResponse(response.getRestStatus(), convertToJson(channel, response, Watch.WITHOUT_AUTH_TOKEN)));
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

    protected void handlePost(String id, RestChannel channel, RestRequest request, Client client) throws IOException {
    }

    @Override
    public String getName() {
        return "Watch Action";
    }

}
