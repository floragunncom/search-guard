/*
 * Copyright 2015-2017 floragunn GmbH
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

package com.floragunn.searchguard.rest;

import static org.opensearch.rest.RestRequest.Method.GET;
import static org.opensearch.rest.RestRequest.Method.POST;

import java.io.IOException;
import java.util.List;

import org.opensearch.client.node.NodeClient;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestStatus;

import com.floragunn.searchguard.auth.BackendRegistry;
import com.google.common.collect.ImmutableList;

public class SearchGuardHealthAction extends BaseRestHandler {

    private final BackendRegistry registry;

    public SearchGuardHealthAction(final Settings settings, final RestController controller, final BackendRegistry registry) {
        super();
        this.registry = registry;
    }

    @Override
    public List<Route> routes() {
        return ImmutableList.of(new Route(GET, "/_searchguard/health"), new Route(POST, "/_searchguard/health"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        return new RestChannelConsumer() {

            final String mode = request.param("mode", "strict");

            @Override
            public void accept(RestChannel channel) throws Exception {
                XContentBuilder builder = channel.newBuilder();
                RestStatus restStatus = RestStatus.OK;
                BytesRestResponse response = null;
                try {

                    String status = "UP";
                    String message = null;

                    builder.startObject();

                    if ("strict".equalsIgnoreCase(mode) && registry.isInitialized() == false) {
                        status = "DOWN";
                        message = "Not initialized";
                        restStatus = RestStatus.SERVICE_UNAVAILABLE;
                    }

                    builder.field("message", message);
                    builder.field("mode", mode);
                    builder.field("status", status);
                    builder.endObject();
                    response = new BytesRestResponse(restStatus, builder);

                } finally {
                    builder.close();
                }

                channel.sendResponse(response);
            }

        };
    }

    @Override
    public String getName() {
        return "Search Guard Health Check";
    }

}
