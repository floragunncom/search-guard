package com.floragunn.signals.api;

import static java.util.Comparator.comparing;
import static org.elasticsearch.rest.RestRequest.Method.DELETE;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

import java.io.IOException;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;

import com.floragunn.searchguard.authc.rest.TenantAwareRestHandler;
import com.floragunn.signals.actions.watch.ackandget.AckAndGetWatchAction;
import com.floragunn.signals.actions.watch.ackandget.AckAndGetWatchRequest;
import com.floragunn.signals.actions.watch.ackandget.AckAndGetWatchResponse;
import com.floragunn.signals.actions.watch.ackandget.Acknowledgement;
import com.google.common.collect.ImmutableList;

public class AckAndGetWatchApiAction extends SignalsBaseRestHandler implements TenantAwareRestHandler {

    private static final Logger log = LogManager.getLogger(AckAndGetWatchApiAction.class);

    public AckAndGetWatchApiAction(Settings settings) {
        super(settings);
    }

    @Override
    public String getName() {
        return "Ack and Get Watch ";
    }

    @Override
    public List<Route> routes() {
        return ImmutableList.of(new Route(PUT, "/_signals/watch/{tenant}/{id}/_ack_and_get"), new Route(PUT, "/_signals/watch/{tenant}/{id}/_ack_and_get/{actionId}"),
            new Route(DELETE, "/_signals/watch/{tenant}/{id}/_ack_and_get"), new Route(DELETE, "/_signals/watch/{tenant}/{id}/_ack_and_get/{actionId}"));
    }


    @Override
    protected final RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {

        final String watchId = request.param("id");
        final String actionId = request.param("actionId");

        return channel -> {

            boolean ack = request.getHttpRequest().method() == PUT;
            client.execute(
                AckAndGetWatchAction.INSTANCE, new AckAndGetWatchRequest(watchId, actionId, ack),
                new ActionListener<AckAndGetWatchResponse>() {

                    @Override
                    public void onResponse(AckAndGetWatchResponse response) {
                        if (response.getStatus() == AckAndGetWatchResponse.Status.SUCCESS) {
                            createCustomRestResponse(channel, response, ack);
                        } else if (response.getStatus() == AckAndGetWatchResponse.Status.NO_SUCH_WATCH) {
                            errorResponse(channel, RestStatus.NOT_FOUND, response.getStatusMessage());
                        } else if (response.getStatus() == AckAndGetWatchResponse.Status.ILLEGAL_STATE) {
                            errorResponse(channel, RestStatus.PRECONDITION_FAILED, response.getStatusMessage());
                        } else if (response.getStatus() == AckAndGetWatchResponse.Status.NO_SUCH_ACTION) {
                            errorResponse(channel, RestStatus.NOT_FOUND, response.getStatusMessage());
                        } else if (response.getStatus() == AckAndGetWatchResponse.Status.NOT_ACKNOWLEDGEABLE) {
                            errorResponse(channel, RestStatus.BAD_REQUEST, response.getStatusMessage());
                        } else {
                            errorResponse(channel, RestStatus.INTERNAL_SERVER_ERROR, response.getStatusMessage());
                        }

                    }

                    @Override
                    public void onFailure(Exception e) {
                        errorResponse(channel, e);
                    }
                });

        };

    }

    private void createCustomRestResponse(RestChannel channel, AckAndGetWatchResponse response, boolean ack) {
        try {
            final XContentBuilder builder = channel.newBuilder();
            builder.prettyPrint();
            builder.humanReadable(true);
            builder.startObject();
            builder.field("status", RestStatus.OK);
            if(ack) {
                appendAckResponseFragment(response, builder);
            } else {
                appendUnackResponseFragment(response, builder);
            }

            builder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        } catch (Exception e) {
            log.error("Cannot build response body for (de)ack and get action", e);
            throw ExceptionsHelper.convertToElastic(e);
        }
    }

    private static void appendUnackResponseFragment(AckAndGetWatchResponse response, XContentBuilder builder) throws IOException {
        builder.array("unacked_action_ids", response.getUnackedActionIds());
    }

    private static void appendAckResponseFragment(AckAndGetWatchResponse response, XContentBuilder builder) throws IOException {
        builder.startArray("acked");
        for (Acknowledgement acknowledgement : response.getSortedAcknowledgements(comparing(Acknowledgement::getAcknowledgeTime))) {
            builder.startObject();
            builder.field("action_id", acknowledgement.getActionId());
            builder.field("by_user", acknowledgement.getAcknowledgeByUser());
            builder.field("on", acknowledgement.getAcknowledgeTime());
            builder.endObject();
        }
        builder.endArray();
    }
}
