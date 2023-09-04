/*
 * Copyright 2019-2022 floragunn GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.floragunn.signals.api;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

import java.io.IOException;
import java.util.List;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequest.Method;
import org.elasticsearch.rest.RestStatus;

import com.floragunn.signals.actions.watch.ack.AckWatchAction;
import com.floragunn.signals.actions.watch.ack.AckWatchRequest;
import com.floragunn.signals.actions.watch.ack.AckWatchResponse;
import com.google.common.collect.ImmutableList;

public class AckWatchApiAction extends SignalsBaseRestHandler {

    public AckWatchApiAction(Settings settings, RestController controller) {
        super(settings);
    }

    @Override
    public List<Route> routes() {
        return ImmutableList.of(new Route(PUT, "/_signals/watch/{tenant}/{id}/_ack"), new Route(PUT, "/_signals/watch/{tenant}/{id}/_ack/{actionId}"),
                new Route(DELETE, "/_signals/watch/{tenant}/{id}/_ack"), new Route(DELETE, "/_signals/watch/{tenant}/{id}/_ack/{actionId}"));
    }

    @Override
    protected final RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {

        final String watchId = request.param("id");
        final String actionId = request.param("actionId");

        return channel -> {

            client.execute(AckWatchAction.INSTANCE, new AckWatchRequest(watchId, actionId, request.getHttpRequest().method() == Method.PUT),
                    new ActionListener<AckWatchResponse>() {

                        @Override
                        public void onResponse(AckWatchResponse response) {
                            if (response.getStatus() == AckWatchResponse.Status.SUCCESS) {
                                response(channel, RestStatus.OK);
                            } else if (response.getStatus() == AckWatchResponse.Status.NO_SUCH_WATCH) {
                                errorResponse(channel, RestStatus.NOT_FOUND, response.getStatusMessage());
                            } else if (response.getStatus() == AckWatchResponse.Status.ILLEGAL_STATE) {
                                errorResponse(channel, RestStatus.PRECONDITION_FAILED, response.getStatusMessage());
                            } else if (response.getStatus() == AckWatchResponse.Status.NO_SUCH_ACTION) {
                                errorResponse(channel, RestStatus.NOT_FOUND, response.getStatusMessage());
                            } else if (response.getStatus() == AckWatchResponse.Status.NOT_ACKNOWLEDGEABLE) {
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

    @Override
    public String getName() {
        return "Ack Watch";
    }

}
