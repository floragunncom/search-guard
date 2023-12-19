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

package com.floragunn.searchguard.rest;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Rest handler which supports requests sent to endpoints where path starts with {@link #SEARCHGUARD_ENDPOINT_PATH_PREFIX}.
 * It should be called when there is no other handler supporting such HTTP method and request path.
 * Returns response with {@link RestStatus#NOT_FOUND} status.
 */
public class NotFoundRestHandler extends BaseRestHandler {

    private static final String SEARCHGUARD_ENDPOINT_PATH_PREFIX = "/_searchguard";

    @Override
    public String getName() {
        return "Search Guard Endpoint Not Found";
    }

    @Override
    public List<Route> routes() {
        return Stream.of(RestRequest.Method.values())
                //org.elasticsearch.rest.RestController disallows
                // handlers for the OPTIONS method when java assertions are enabled
                .filter(method -> RestRequest.Method.OPTIONS != method)
                .flatMap(method -> IntStream.rangeClosed(1, 100).boxed()
                        .map(numberOfPathWildcardComponents -> {
                            String wildcardPathComponents = String.join("", Collections.nCopies(numberOfPathWildcardComponents, "/*"));
                            return new Route(method, SEARCHGUARD_ENDPOINT_PATH_PREFIX.concat(wildcardPathComponents));
                        }))
                .collect(Collectors.toList());
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        request.content();
        return restChannel -> restChannel.sendResponse(
                new BytesRestResponse(RestStatus.NOT_FOUND, XContentType.JSON.mediaType(), "")
        );
    }
}
