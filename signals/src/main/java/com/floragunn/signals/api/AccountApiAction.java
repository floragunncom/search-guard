package com.floragunn.signals.api;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

import java.io.IOException;
import java.util.List;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse.Result;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import com.floragunn.signals.actions.account.delete.DeleteAccountAction;
import com.floragunn.signals.actions.account.delete.DeleteAccountRequest;
import com.floragunn.signals.actions.account.delete.DeleteAccountResponse;
import com.floragunn.signals.actions.account.get.GetAccountAction;
import com.floragunn.signals.actions.account.get.GetAccountRequest;
import com.floragunn.signals.actions.account.get.GetAccountResponse;
import com.floragunn.signals.actions.account.put.PutAccountAction;
import com.floragunn.signals.actions.account.put.PutAccountRequest;
import com.floragunn.signals.actions.account.put.PutAccountResponse;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;

public class AccountApiAction extends SignalsBaseRestHandler {

    public AccountApiAction(final Settings settings, final RestController controller) {
        super(settings);
    }

    @Override
    public List<Route> routes() {
        return ImmutableList.of(new Route(GET, "/_signals/account/{type}/{id}"), new Route(PUT, "/_signals/account/{type}/{id}"),
                new Route(DELETE, "/_signals/account/{type}/{id}"));
    }

    @Override
    protected final RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {

        String accountType = request.param("type");

        if (accountType == null) {
            return channel -> errorResponse(channel, RestStatus.BAD_REQUEST, "No type specified");
        }

        String id = request.param("id");

        if (Strings.isNullOrEmpty(id)) {
            return channel -> errorResponse(channel, RestStatus.BAD_REQUEST, "No id specified");
        }

        switch (request.method()) {
        case GET:
            return handleGet(accountType, id, request, client);
        case PUT:
            return handlePut(accountType, id, request, client);
        case DELETE:
            return handleDelete(accountType, id, request, client);
        default:
            throw new IllegalArgumentException(request.method() + " not supported");
        }
    }

    protected RestChannelConsumer handleGet(String accountType, String id, RestRequest request, Client client) throws IOException {

        return channel -> client.execute(GetAccountAction.INSTANCE, new GetAccountRequest(accountType, id), new ActionListener<GetAccountResponse>() {

            @Override
            public void onResponse(GetAccountResponse response) {
                if (response.isExists()) {
                    channel.sendResponse(new RestResponse(RestStatus.OK, convertToJson(channel, response, ToXContent.EMPTY_PARAMS)));
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

    protected RestChannelConsumer handleDelete(String accountType, String id, RestRequest request, Client client) throws IOException {

        return channel -> client.execute(DeleteAccountAction.INSTANCE, new DeleteAccountRequest(accountType, id),
                new ActionListener<DeleteAccountResponse>() {

                    @Override
                    public void onResponse(DeleteAccountResponse response) {
                        if (response.getResult() == DeleteAccountResponse.Result.DELETED) {
                            channel.sendResponse(new RestResponse(RestStatus.OK, convertToJson(channel, response, ToXContent.EMPTY_PARAMS)));
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

    protected RestChannelConsumer handlePut(String accountType, String id, RestRequest request, Client client) throws IOException {

        ReleasableBytesReference content = request.content();

        if (request.getXContentType() != XContentType.JSON && request.getXContentType() != XContentType.VND_JSON) {
            return channel -> errorResponse(channel, RestStatus.UNSUPPORTED_MEDIA_TYPE, "Accounts must be of content type application/json");
        }

        return channel -> client.execute(PutAccountAction.INSTANCE, new PutAccountRequest(accountType, id, content, XContentType.JSON),
                ActionListener.withRef(
                        new ActionListener<PutAccountResponse>() {
                            @Override
                            public void onResponse(PutAccountResponse response) {
                                if (response.getResult() == Result.CREATED || response.getResult() == Result.UPDATED) {
                                    channel.sendResponse(
                                            new RestResponse(response.getRestStatus(), convertToJson(channel, response, ToXContent.EMPTY_PARAMS)));
                                } else {
                                    errorResponse(channel, response.getRestStatus(), response.getMessage(), response.getDetailJsonDocument());
                                }
                            }

                            @Override
                            public void onFailure(Exception e) {
                        errorResponse(channel, e);
                    }
                    }, content));

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
        return "Account Action";
    }

}
