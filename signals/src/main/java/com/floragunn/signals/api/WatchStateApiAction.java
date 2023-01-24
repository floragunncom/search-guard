/*
 * Copyright 2023 floragunn GmbH
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

import static org.elasticsearch.rest.RestRequest.Method.GET;

import com.floragunn.searchguard.authc.rest.TenantAwareRestHandler;
import com.floragunn.signals.actions.watch.state.get.GetWatchStateAction;
import com.floragunn.signals.actions.watch.state.get.GetWatchStateRequest;
import com.floragunn.signals.actions.watch.state.get.GetWatchStateResponse;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;

public class WatchStateApiAction extends SignalsBaseRestHandler implements TenantAwareRestHandler {

    public WatchStateApiAction(Settings settings, RestController controller) {
        super(settings);
    }

    @Override
    public List<Route> routes() {
        return ImmutableList.of(new Route(GET, "/_signals/watch/{tenant}/{id}/_state"));
    }

    @Override
    protected final RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {

        final String watchId = request.param("id");

        return channel -> {

            // TODO action listener which handles exceptions within onResponse and onFailure
            client.execute(GetWatchStateAction.INSTANCE, new GetWatchStateRequest(Collections.singletonList(watchId)),
                    new ActionListener<GetWatchStateResponse>() {

                        @Override
                        public void onResponse(GetWatchStateResponse response) {
                            BytesReference statusDoc = response.getWatchToStatusMap().get(watchId);

                            if (statusDoc != null) {
                                channel.sendResponse(new BytesRestResponse(RestStatus.OK, "application/json", statusDoc));
                            } else {
                                errorResponse(channel, RestStatus.NOT_FOUND, "No such watch: " + watchId);
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
        return "Watch State";
    }

}
