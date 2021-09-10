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

package com.floragunn.searchguard.configuration.api;

import static org.opensearch.rest.RestRequest.Method.GET;
import static org.opensearch.rest.RestRequest.Method.PUT;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.ActionResponse;
import org.opensearch.action.ActionType;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.client.Client;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.Strings;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.StatusToXContentObject;
import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestStatus;
import org.opensearch.rest.action.RestStatusToXContentListener;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.floragunn.codova.documents.DocType;
import com.floragunn.codova.documents.DocType.UnknownContentTypeException;
import com.floragunn.codova.documents.DocUtils;
import com.floragunn.codova.documents.UnparsedDoc;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.errors.InvalidAttributeValue;
import com.floragunn.codova.validation.errors.JsonValidationError;
import com.floragunn.codova.validation.errors.MissingAttribute;
import com.floragunn.codova.validation.errors.ValidationError;
import com.floragunn.searchguard.configuration.ConfigUpdateException;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.sgconf.impl.CType;
import com.floragunn.searchguard.sgconf.impl.SgDynamicConfiguration;
import com.floragunn.searchsupport.client.rest.Responses;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class BulkConfigApi {
    private static final Logger log = LogManager.getLogger(BulkConfigApi.class);

    public static class GetAction extends ActionType<GetAction.Response> {

        public static final GetAction INSTANCE = new GetAction();
        public static final String NAME = "cluster:admin:searchguard:config/bulk/get";

        protected GetAction() {
            super(NAME, in -> new Response(in));
        }

        public static class Request extends ActionRequest {

            private boolean pretty;
            private final Map<String, Object> options;

            public Request() {
                super();
                options = new HashMap<>();
            }

            public Request(StreamInput in) throws IOException {
                super(in);
                this.pretty = in.readBoolean();
                this.options = in.readMap(StreamInput::readString, StreamInput::readGenericValue);
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                out.writeBoolean(pretty);
                out.writeMap(options, StreamOutput::writeString, StreamOutput::writeGenericValue);
            }

            @Override
            public ActionRequestValidationException validate() {
                return null;
            }
        }

        public static class Response extends ActionResponse implements StatusToXContentObject {

            private RestStatus restStatus;
            private String error;
            private String config;

            public Response() {
            }

            public Response(String config) {
                this.restStatus = RestStatus.OK;
                this.config = config;
            }

            public Response(StreamInput in) throws IOException {
                super(in);
                this.restStatus = in.readEnum(RestStatus.class);
                this.error = in.readOptionalString();
                this.config = in.readOptionalString();
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                out.writeEnum(this.restStatus);
                out.writeOptionalString(this.error);
                out.writeOptionalString(this.config);
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {

                if (error != null) {
                    builder.startObject();
                    builder.field("error", error);
                    builder.endObject();
                } else {
                    builder.rawValue(new ByteArrayInputStream(config.getBytes(Charsets.UTF_8)), XContentType.JSON);
                }

                return builder;
            }

            @Override
            public RestStatus status() {
                return restStatus;
            }

        }

        public static class TransportAction extends HandledTransportAction<Request, Response> {

            private final ThreadPool threadPool;
            private final ConfigurationRepository configurationRepository;

            private final static ToXContent.Params OMIT_DEFAULTS_PARAMS = new ToXContent.MapParams(ImmutableMap.of("omit_defaults", "true"));

            @Inject
            public TransportAction(TransportService transportService, ThreadPool threadPool, ActionFilters actionFilters,
                    ConfigurationRepository configurationRepository) {
                super(GetAction.NAME, transportService, actionFilters, Request::new);

                this.threadPool = threadPool;
                this.configurationRepository = configurationRepository;
            }

            @Override
            protected final void doExecute(Task task, Request request, ActionListener<Response> listener) {
                threadPool.generic().submit(() -> {

                    try (XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent()).prettyPrint().humanReadable(true)) {
                        Map<CType, SgDynamicConfiguration<?>> configMap = configurationRepository
                                .getConfigurationsFromIndex(Arrays.asList(CType.values()), true);

                        builder.startObject();

                        for (Map.Entry<CType, SgDynamicConfiguration<?>> entry : configMap.entrySet()) {
                            SgDynamicConfiguration<?> config = entry.getValue();

                            builder.field(entry.getKey().toLCString());
                            if (config != null) {

                                builder.startObject();
                                builder.field("content");
                                if (config.getUninterpolatedJson() != null) {
                                    builder.rawValue(new ByteArrayInputStream(config.getUninterpolatedJson().getBytes(Charsets.UTF_8)),
                                            XContentType.JSON);
                                } else {
                                    config.toXContent(builder, OMIT_DEFAULTS_PARAMS);
                                }
                                builder.field("_version", config.getDocVersion());
                                builder.field("_seq_no", config.getSeqNo());
                                builder.field("_primary_term", config.getPrimaryTerm());
                                builder.endObject();
                            } else {
                                builder.nullValue();
                            }

                        }

                        builder.endObject();

                        String resultJson = Strings.toString(builder);

                        listener.onResponse(new Response(resultJson));
                    } catch (Exception e) {
                        log.error("Error in " + this, e);
                        listener.onFailure(e);
                    }
                });
            }
        }
    }

    public static class UpdateAction extends ActionType<UpdateAction.Response> {

        public static final UpdateAction INSTANCE = new UpdateAction();
        public static final String NAME = "cluster:admin:searchguard:config/bulk/update";

        protected UpdateAction() {
            super(NAME, in -> new Response(in));
        }

        public static class Request extends ActionRequest {

            private final UnparsedDoc config;
            private final Map<String, Object> options;

            public Request(UnparsedDoc config, Map<String, Object> options) {
                super();
                this.config = config;
                this.options = options;
            }

            public Request(StreamInput in) throws IOException {
                super(in);
                try {
                    this.config = UnparsedDoc.fromString(in.readString());
                } catch (IllegalArgumentException | UnknownContentTypeException e) {
                    throw new IOException(e);
                }
                this.options = in.readMap(StreamInput::readString, StreamInput::readGenericValue);
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                out.writeString(config.toString());
                out.writeMap(options, StreamOutput::writeString, StreamOutput::writeGenericValue);
            }

            @Override
            public ActionRequestValidationException validate() {
                return null;
            }

            public static Request parse(BytesReference bytesReference, DocType docType) throws ConfigValidationException {
                String bodyAsString = new String(BytesReference.toBytes(bytesReference), Charsets.UTF_8);
                return new Request(new UnparsedDoc(bodyAsString, docType), new HashMap<>());
            }

            public Map<String, Object> getOptions() {
                return options;
            }

            public UnparsedDoc getConfig() {
                return config;
            }
        }

        public static class Response extends ActionResponse implements StatusToXContentObject {

            private RestStatus restStatus;
            private String error;
            private String message;
            private String detailJson;

            public Response() {
            }

            public Response(RestStatus restStatus, String message) {
                this.restStatus = restStatus;
                this.message = message;
            }

            public Response(RestStatus restStatus, String error, String detailJson) {
                this.restStatus = restStatus;
                this.error = error;
                this.detailJson = detailJson;
            }

            public Response(StreamInput in) throws IOException {
                super(in);
                this.restStatus = in.readEnum(RestStatus.class);
                this.message = in.readOptionalString();
                this.error = in.readOptionalString();
                this.detailJson = in.readOptionalString();
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                out.writeEnum(this.restStatus);
                out.writeOptionalString(this.message);
                out.writeOptionalString(this.error);
                out.writeOptionalString(this.detailJson);
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {

                builder.startObject();

                if (message != null) {
                    builder.field("message", message);
                }

                if (error != null) {
                    builder.field("error", error);
                }

                if (detailJson != null) {
                    builder.rawField("detail", new ByteArrayInputStream(detailJson.getBytes(Charsets.UTF_8)), XContentType.JSON);
                }

                builder.endObject();

                return builder;
            }

            @Override
            public RestStatus status() {
                return restStatus;
            }

        }

        public static class TransportAction extends HandledTransportAction<Request, Response> {

            private final ConfigurationRepository configurationRepository;
            private final ThreadPool threadPool;

            @Inject
            public TransportAction(TransportService transportService, ThreadPool threadPool, ActionFilters actionFilters,
                    ConfigurationRepository configurationRepository, Settings settings, Client client) {
                super(UpdateAction.NAME, transportService, actionFilters, Request::new);

                this.configurationRepository = configurationRepository;
                this.threadPool = threadPool;
            }

            @Override
            protected final void doExecute(Task task, Request request, ActionListener<Response> listener) {
                threadPool.generic().submit(() -> {
                    try {
                        this.configurationRepository.update(parseConfigJson(request.getConfig()));
                        listener.onResponse(new Response(RestStatus.OK, "Configuration has been updated"));
                    } catch (ConfigValidationException e) {
                        listener.onResponse(new Response(RestStatus.BAD_REQUEST, e.getMessage(), e.getValidationErrors().toJsonString()));
                    } catch (ConfigUpdateException e) {
                        log.error("Error while updating configuration", e);
                        listener.onResponse(new Response(RestStatus.INTERNAL_SERVER_ERROR, e.getMessage(), e.getDetailsAsJson()));
                    } catch (Exception e) {
                        log.error("Error while updating configuration", e);
                        listener.onFailure(e);
                    }
                });
            }

            private Map<CType, Map<String, Object>> parseConfigJson(UnparsedDoc unparsedDoc) throws ConfigValidationException {
                Map<String, Object> parsedJson;

                try {
                    parsedJson = unparsedDoc.parseAsMap();
                } catch (JsonProcessingException e) {
                    throw new ConfigValidationException(new JsonValidationError(null, e));
                }

                ValidationErrors validationErrors = new ValidationErrors();
                Map<CType, Map<String, Object>> configTypeToConfigMap = new HashMap<>();

                for (String configTypeName : parsedJson.keySet()) {
                    CType ctype;

                    try {
                        ctype = CType.valueOf(configTypeName.toUpperCase());
                    } catch (IllegalArgumentException e) {
                        validationErrors.add(new ValidationError(configTypeName, "Invalid config type: " + configTypeName));
                        continue;
                    }

                    Object value = parsedJson.get(configTypeName);

                    if (!(value instanceof Map)) {
                        validationErrors.add(new InvalidAttributeValue(configTypeName, value, "A config JSON document"));
                        continue;
                    }

                    Object content = ((Map<?, ?>) value).get("content");

                    if (content == null) {
                        validationErrors.add(new MissingAttribute(configTypeName + ".content"));
                        continue;
                    }

                    if (!(content instanceof Map)) {
                        validationErrors.add(new InvalidAttributeValue(configTypeName + ".content", content, "A config JSON document"));
                        continue;
                    }

                    Map<String, Object> contentMap = DocUtils.toStringKeyedMap((Map<?, ?>) content);

                    configTypeToConfigMap.put(ctype, contentMap);
                }

                validationErrors.throwExceptionForPresentErrors();

                return configTypeToConfigMap;
            }
        }
    }

    public static class RestAction extends BaseRestHandler {
        @Override
        public List<Route> routes() {
            return ImmutableList.of(new Route(GET, "/_searchguard/config"), new Route(PUT, "/_searchguard/config"));
        }

        @Override
        public String getName() {
            return "Search Guard Config Management API for retrieving and updating all config types in one batch";
        }

        @Override
        protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
            if (request.method() == GET) {
                return handleGet(request, client);
            } else if (request.method() == PUT) {
                return handlePut(request, client);
            } else {
                return (RestChannel channel) -> Responses.sendError(channel, RestStatus.METHOD_NOT_ALLOWED,
                        "Method not allowed: " + request.method());
            }
        }

        private RestChannelConsumer handleGet(RestRequest request, NodeClient client) {

            try {
                return channel -> client.execute(GetAction.INSTANCE, new GetAction.Request(),
                        new RestStatusToXContentListener<GetAction.Response>(channel));
            } catch (Exception e) {
                log.warn("Error while handling request", e);
                return channel -> Responses.sendError(channel, e);
            }
        }

        private RestChannelConsumer handlePut(RestRequest restRequest, NodeClient client) {
            try {
                UpdateAction.Request request = UpdateAction.Request.parse(restRequest.requiredContent(),
                        DocType.getByContentType(restRequest.getXContentType().mediaType()));

                return channel -> client.execute(UpdateAction.INSTANCE, request, new RestStatusToXContentListener<UpdateAction.Response>(channel));
            } catch (Exception e) {
                log.warn("Error while handling request", e);
                return channel -> Responses.sendError(channel, e);
            }
        }

    }
}
