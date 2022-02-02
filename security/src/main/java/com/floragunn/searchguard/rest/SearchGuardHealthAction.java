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

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

import java.io.IOException;
import java.util.List;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;

import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.google.common.collect.ImmutableList;

public class SearchGuardHealthAction extends BaseRestHandler {

    private final ConfigurationRepository configRepository;

    public SearchGuardHealthAction(final Settings settings, final RestController controller, ConfigurationRepository configRepository) {
        super();
        this.configRepository = configRepository;
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

                    if ("strict".equalsIgnoreCase(mode) && configRepository.isInitialized() == false) {
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
