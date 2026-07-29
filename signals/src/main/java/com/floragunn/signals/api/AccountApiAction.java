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
import com.floragunn.signals.actions.account.delete.TenantDeleteAccountAction;
import com.floragunn.signals.actions.account.get.GetAccountAction;
import com.floragunn.signals.actions.account.get.GetAccountRequest;
import com.floragunn.signals.actions.account.get.GetAccountResponse;
import com.floragunn.signals.actions.account.get.TenantGetAccountAction;
import com.floragunn.signals.actions.account.put.PutAccountAction;
import com.floragunn.signals.actions.account.put.PutAccountRequest;
import com.floragunn.signals.actions.account.put.PutAccountResponse;
import com.floragunn.signals.actions.account.put.TenantPutAccountAction;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;

public class AccountApiAction extends SignalsTenantAwareRestHandler {

    public AccountApiAction(final Settings settings, final RestController controller) {
        super(settings);
    }

    @Override
    public List<Route> routes() {
        // The two route shapes represent /{accountType}/{accountId} and /{tenant}/{accountType}/{accountId}.
        // Elasticsearch's PathTrie requires wildcards at the same path position to have the same name across all routes. Thus, the first two
        // wildcard names deliberately describe both possible meanings instead of using the otherwise misleading names "tenant" and "type".
        return ImmutableList.of(new Route(GET, "/_signals/account/{tenantOrAccountType}/{accountTypeOrId}"),
                new Route(PUT, "/_signals/account/{tenantOrAccountType}/{accountTypeOrId}"),
                new Route(DELETE, "/_signals/account/{tenantOrAccountType}/{accountTypeOrId}"),
                new Route(GET, "/_signals/account/{tenantOrAccountType}/{accountTypeOrId}/{accountId}"),
                new Route(PUT, "/_signals/account/{tenantOrAccountType}/{accountTypeOrId}/{accountId}"),
                new Route(DELETE, "/_signals/account/{tenantOrAccountType}/{accountTypeOrId}/{accountId}"));
    }

    @Override
    protected final RestChannelConsumer getRestChannelConsumer(RestRequest request, NodeClient client) throws IOException {

        String tenantOrAccountType = request.param("tenantOrAccountType");
        String accountTypeOrId = request.param("accountTypeOrId");
        String accountId = request.param("accountId");
        // Only the tenant-scoped route has the third positional parameter. The tenant itself is deliberately not copied into the transport
        // request: SignalsTenantParamResolver puts it into User.requestedTenant, where it is authorized and later resolved by the transport action.
        boolean tenantScoped = accountId != null;
        String accountType = tenantScoped ? accountTypeOrId : tenantOrAccountType;
        String id = tenantScoped ? accountId : accountTypeOrId;

        if (accountType == null) {
            return channel -> errorResponse(channel, RestStatus.BAD_REQUEST, "No type specified");
        }

        if (Strings.isNullOrEmpty(id)) {
            return channel -> errorResponse(channel, RestStatus.BAD_REQUEST, "No id specified");
        }

        switch (request.method()) {
        case GET:
            return handleGet(accountType, id, tenantScoped, request, client);
        case PUT:
            return handlePut(accountType, id, tenantScoped, request, client);
        case DELETE:
            return handleDelete(accountType, id, tenantScoped, request, client);
        default:
            throw new IllegalArgumentException(request.method() + " not supported");
        }
    }

    protected RestChannelConsumer handleGet(String accountType, String id, boolean tenantScoped, RestRequest request, Client client)
            throws IOException {

        return channel -> client.execute(tenantScoped ? TenantGetAccountAction.INSTANCE : GetAccountAction.INSTANCE,
                new GetAccountRequest(accountType, id), new ActionListener<GetAccountResponse>() {

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

    protected RestChannelConsumer handleDelete(String accountType, String id, boolean tenantScoped, RestRequest request, Client client)
            throws IOException {

        return channel -> client.execute(tenantScoped ? TenantDeleteAccountAction.INSTANCE : DeleteAccountAction.INSTANCE,
                new DeleteAccountRequest(accountType, id),
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

    protected RestChannelConsumer handlePut(String accountType, String id, boolean tenantScoped, RestRequest request, Client client)
            throws IOException {

        ReleasableBytesReference content = request.content();

        if (request.getXContentType() != XContentType.JSON && request.getXContentType() != XContentType.VND_JSON) {
            return channel -> errorResponse(channel, RestStatus.UNSUPPORTED_MEDIA_TYPE, "Accounts must be of content type application/json");
        }

        return channel -> client.execute(tenantScoped ? TenantPutAccountAction.INSTANCE : PutAccountAction.INSTANCE,
                new PutAccountRequest(accountType, id, content, XContentType.JSON),
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
