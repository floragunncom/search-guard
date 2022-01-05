/*
 * Copyright 2021 floragunn GmbH
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

package com.floragunn.searchsupport.action;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestResponseListener;

import com.floragunn.codova.documents.ContentType;
import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Format;
import com.floragunn.codova.documents.DocWriter;
import com.floragunn.codova.documents.UnparsedDocument;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.searchsupport.client.rest.Responses;

public class RestApi extends BaseRestHandler {
    private static final Logger log = LogManager.getLogger(RestApi.class);

    private final Map<RestRequest.Method, List<Endpoint>> methodToEndpointMap = new EnumMap<>(RestRequest.Method.class);
    private String name;

    public RestApi() {

    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public final List<Route> routes() {
        ArrayList<Route> routes = new ArrayList<>();

        for (List<Endpoint> endpoints : methodToEndpointMap.values()) {
            for (Endpoint endpoint : endpoints) {
                for (String route : endpoint.routes) {
                    routes.add(new Route(endpoint.platformMethod, route));
                }
            }
        }

        return routes;
    }

    public Endpoint handlesGet(String routes) {
        return new Endpoint(Method.GET, routes);
    }

    public Endpoint handlesPost(String routes) {
        return new Endpoint(Method.POST, routes);
    }

    public Endpoint handlesPut(String routes) {
        return new Endpoint(Method.PUT, routes);
    }

    public Endpoint handlesPatch(String routes) {
        return new Endpoint(Method.PATCH, routes);
    }

    public Endpoint handlesDelete(String routes) {
        return new Endpoint(Method.DELETE, routes);
    }

    public RestApi name(String name) {
        this.name = name;
        return this;
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        Endpoint endpoint = chooseEndpoint(methodToEndpointMap.get(request.method()), request.params());

        if (endpoint != null) {
            return endpoint.handler.apply(request, client);
        } else {
            return (RestChannel channel) -> Responses.sendError(channel, RestStatus.METHOD_NOT_ALLOWED, "Method not allowed: " + request.method());
        }
    }

    private void addEndpoint(Endpoint endpoint) {
        methodToEndpointMap.computeIfAbsent(endpoint.platformMethod, (k) -> new ArrayList<Endpoint>(4)).add(endpoint);
    }

    private Endpoint chooseEndpoint(List<Endpoint> endpoints, Map<String, String> params) {
        if (endpoints == null || endpoints.size() == 0) {
            return null;
        } else if (endpoints.size() == 1) {
            return endpoints.get(0);
        } else {
            Endpoint bestMatch = null;
            int bestMatchSize = -1;

            for (Endpoint endpoint : endpoints) {
                int match = endpoint.matchParams(params);

                if (match != -1 && match > bestMatchSize) {
                    bestMatch = endpoint;
                    bestMatchSize = match;
                }
            }

            if (log.isDebugEnabled()) {
                if (bestMatch != null) {
                    log.debug("Chose " + bestMatch + " based on " + params + " and " + bestMatch.requiredParams);
                } else {
                    log.debug("Could not find endpoint for params " + params + "; params of available endpoints: "
                            + endpoints.stream().map((e) -> e.requiredParams).collect(Collectors.toList()));
                }
            }

            return bestMatch;
        }
    }

    public class Endpoint {
        private final Method method;
        private final RestRequest.Method platformMethod;
        private final List<String> routes;
        private BiFunction<RestRequest, NodeClient, RestChannelConsumer> handler;
        private final Set<String> requiredParams;

        public Endpoint(Method method, String route) {
            this.method = method;
            this.platformMethod = RestRequest.Method.valueOf(method.toString());
            this.routes = Collections.singletonList(route);
            this.requiredParams = getPathParamsFromRoute(route);
        }

        public RestApi with(BiFunction<RestRequest, NodeClient, RestChannelConsumer> handler) {
            this.handler = handler;
            addEndpoint(this);
            return RestApi.this;
        }

        public <RequestType extends Action.Request, ResponseType extends Action.Response> RestApi with(Action<RequestType, ResponseType> action) {
            if (action == null) {
                throw new IllegalArgumentException("action must not be null");
            }

            return with((restRequest, client) -> {
                try {
                    boolean prettyPrintResponse = restRequest.paramAsBoolean("pretty", false);

                    UnparsedDocument<?> unparsedDoc = null;

                    if (restRequest.hasContent()) {
                        ContentType contentType = ContentType.parseHeader(restRequest.header("Content-Type"));

                        if (contentType == null) {
                            return channel -> Responses.sendError(channel, RestStatus.BAD_REQUEST, "Content-Type header is missing");
                        }

                        unparsedDoc = UnparsedDocument.from(BytesReference.toBytes(restRequest.content()), contentType);
                    }

                    RequestType transportRequest = action.parseRequest(new Action.UnparsedMessage(unparsedDoc, DocNode.EMPTY));
                    
                    String ifMatchHeader = restRequest.header("If-Match");
                    
                    if (ifMatchHeader != null) {
                        transportRequest.ifMatch(ifMatchHeader);
                    }
                    
                    String ifNoneMatchHeader = restRequest.header("If-None-Match");
                    
                    if (ifNoneMatchHeader != null) {
                        transportRequest.ifNoneMatch(ifNoneMatchHeader);
                    }

                    return channel -> client.execute(action, transportRequest, new RestResponseListener<ResponseType>(channel) {

                        @Override
                        public RestResponse buildResponse(ResponseType response) throws Exception {
                            Format responseDocType = Format.JSON;

                            RestResponse restResponse = new BytesRestResponse(response.status(), responseDocType.getMediaType(),
                                    DocWriter.format(responseDocType).pretty(prettyPrintResponse).writeAsString(response));
                            
                            if (response.getConcurrencyControlEntityTag() != null) {
                                restResponse.addHeader("ETag", response.getConcurrencyControlEntityTag());
                            }
                            
                            return restResponse;
                        }

                    });
                } catch (Exception e) {
                    log.warn("Error while handling request", e);
                    return channel -> Responses.sendError(channel, e);
                }
            });

        }

        public <RequestType extends Action.Request, ResponseType extends Action.Response> RestApi with(Action<RequestType, ResponseType> action,
                RestRequestParser<RequestType> requestParser) {
            if (action == null) {
                throw new IllegalArgumentException("action must not be null");
            }

            if (requestParser == null) {
                throw new IllegalArgumentException("requestParser must not be null");
            }

            return with((restRequest, client) -> {
                try {
                    boolean prettyPrintResponse = restRequest.paramAsBoolean("pretty", false);

                    UnparsedDocument<?> unparsedDoc = null;

                    if (restRequest.hasContent()) {
                        ContentType contentType = ContentType.parseHeader(
                                restRequest.header("X-SG-Original-Content-Type") != null ? restRequest.header("X-SG-Original-Content-Type")
                                        : restRequest.header("Content-Type"));

                        if (contentType == null) {
                            return channel -> Responses.sendError(channel, RestStatus.BAD_REQUEST, "Content-Type header is missing");
                        }

                        unparsedDoc = UnparsedDocument.from(BytesReference.toBytes(restRequest.content()), contentType);
                    }

                    RequestType transportRequest = requestParser.parse(new RestRequestParams(restRequest), unparsedDoc);
                    
                    String ifMatchHeader = restRequest.header("If-Match");
                    
                    if (ifMatchHeader != null) {
                        transportRequest.ifMatch(ifMatchHeader);
                    }
                    
                    String ifNoneMatchHeader = restRequest.header("If-None-Match");
                    
                    if (ifNoneMatchHeader != null) {
                        transportRequest.ifNoneMatch(ifNoneMatchHeader);
                    }

                    if (log.isDebugEnabled()) {
                        log.debug("Parsed request for " + this + ": " + transportRequest);
                    }

                    return channel -> client.execute(action, transportRequest, new RestResponseListener<ResponseType>(channel) {

                        @Override
                        public RestResponse buildResponse(ResponseType response) throws Exception {
                            Format responseDocType = Format.JSON;

                            RestResponse restResponse = new BytesRestResponse(response.status(), responseDocType.getMediaType(),
                                    DocWriter.format(responseDocType).pretty(prettyPrintResponse).writeAsString(response));
                            
                            if (response.getConcurrencyControlEntityTag() != null) {
                                restResponse.addHeader("ETag", response.getConcurrencyControlEntityTag());
                            }
                            
                            return restResponse;
                        }

                    });
                } catch (Exception e) {
                    log.warn("Error while handling request", e);
                    return channel -> Responses.sendError(channel, e);
                }
            });
        }

        @Override
        public String toString() {
            return "Endpoint [method=" + method + ", routes=" + routes + "]";
        }

        private int matchParams(Map<String, String> actualParams) {
            if (requiredParams.size() == 0) {
                return 0;
            }

            int matchingParams = 0;

            for (String requiredParam : requiredParams) {
                if (actualParams.containsKey(requiredParam)) {
                    matchingParams++;
                } else {
                    return -1;
                }
            }

            return matchingParams;
        }
    }

    public static enum Method {
        GET, POST, PUT, DELETE, OPTIONS, HEAD, PATCH;
    }
    
    @FunctionalInterface
    public static interface RestRequestParser<RequestType extends Action.Request> {
        RequestType parse(Map<String, String> requestUrlParams, UnparsedDocument<?> requestBody) throws ConfigValidationException;
    }

    private static class RestRequestParams implements Map<String, String> {
        private final RestRequest request;

        RestRequestParams(RestRequest request) {
            this.request = request;
        }

        @Override
        public int size() {
            return request.params().size();
        }

        @Override
        public boolean isEmpty() {
            return request.params().isEmpty();
        }

        @Override
        public boolean containsKey(Object key) {
            if (key instanceof String) {
                return request.hasParam((String) key);
            } else {
                return false;
            }
        }

        @Override
        public boolean containsValue(Object value) {
            return request.params().containsValue(value);

        }

        @Override
        public String get(Object key) {
            if (key instanceof String) {
                return request.param((String) key);
            } else {
                return null;
            }
        }

        @Override
        public String put(String key, String value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String remove(Object key) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putAll(Map<? extends String, ? extends String> m) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void clear() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Set<String> keySet() {
            return request.params().keySet();

        }

        @Override
        public Collection<String> values() {
            return request.params().values();

        }

        @Override
        public Set<Entry<String, String>> entrySet() {
            return request.params().entrySet();

        }
    }

    private static final Pattern PATH_PARAM_PATTERN = Pattern.compile("\\{([^\\}]*)\\}");

    private static Set<String> getPathParamsFromRoute(String route) {
        if (route.indexOf('{') == -1) {
            return Collections.emptySet();
        }

        Set<String> result = new HashSet<>();
        Matcher matcher = PATH_PARAM_PATTERN.matcher(route);

        while (matcher.find()) {
            result.add(matcher.group(1));
        }

        return result;
    }
}