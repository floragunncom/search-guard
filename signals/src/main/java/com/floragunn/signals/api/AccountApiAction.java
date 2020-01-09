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
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;

import com.floragunn.signals.accounts.AccountType;
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

public class AccountApiAction extends SignalsBaseRestHandler {

    public AccountApiAction(final Settings settings, final RestController controller) {
        super(settings);
        controller.registerHandler(GET, "/_signals/account/{type}/{id}", this);
        controller.registerHandler(PUT, "/_signals/account/{type}/{id}", this);
        controller.registerHandler(DELETE, "/_signals/account/{type}/{id}", this);

    }

    @Override
    protected final RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {

        String type = request.param("type");

        if (type == null) {
            return channel -> {
                errorResponse(channel, RestStatus.BAD_REQUEST, "No type specified");
            };
        }

        AccountType accountType = AccountType.getByName(type);

        if (accountType == null) {
            return channel -> {
                errorResponse(channel, RestStatus.BAD_REQUEST, "Unknown account type specified: " + type);
            };
        }

        String id = request.param("id");

        if (Strings.isNullOrEmpty(id)) {
            return channel -> {
                errorResponse(channel, RestStatus.BAD_REQUEST, "No id specified");
            };
        }

        return channel -> {
            handleApiRequest(accountType, id, channel, request, client);
        };
    }

    protected void handleApiRequest(AccountType accountType, String id, RestChannel channel, RestRequest request, Client client) throws IOException {

        switch (request.method()) {
        case GET:
            handleGet(accountType, id, channel, request, client);
            break;
        case PUT:
            handlePut(accountType, id, channel, request, client);
            break;
        case DELETE:
            handleDelete(accountType, id, channel, request, client);
            break;
        default:
            throw new IllegalArgumentException(request.method() + " not supported");
        }
    }

    protected void handleGet(AccountType accountType, String id, RestChannel channel, RestRequest request, Client client) throws IOException {

        client.execute(GetAccountAction.INSTANCE, new GetAccountRequest(accountType, id), new ActionListener<GetAccountResponse>() {

            @Override
            public void onResponse(GetAccountResponse response) {
                if (response.isExists()) {
                    channel.sendResponse(new BytesRestResponse(RestStatus.OK, convertToJson(channel, response, ToXContent.EMPTY_PARAMS)));
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

    protected void handleDelete(AccountType accountType, String id, RestChannel channel, RestRequest request, Client client) throws IOException {

        client.execute(DeleteAccountAction.INSTANCE, new DeleteAccountRequest(accountType, id), new ActionListener<DeleteAccountResponse>() {

            @Override
            public void onResponse(DeleteAccountResponse response) {
                if (response.getResult() == DeleteAccountResponse.Result.DELETED) {
                    channel.sendResponse(new BytesRestResponse(RestStatus.OK, convertToJson(channel, response, ToXContent.EMPTY_PARAMS)));
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

    protected void handlePut(AccountType accountType, String id, RestChannel channel, RestRequest request, Client client) throws IOException {

        if (request.getXContentType() != XContentType.JSON) {
            errorResponse(channel, RestStatus.UNPROCESSABLE_ENTITY, "Accounts must be of content type application/json");
            return;
        }

        client.execute(PutAccountAction.INSTANCE, new PutAccountRequest(accountType, id, request.content(), XContentType.JSON),
                new ActionListener<PutAccountResponse>() {

                    @Override
                    public void onResponse(PutAccountResponse response) {
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
        return "Account Action";
    }

}
