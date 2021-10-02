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

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.ToXContent;

import com.fasterxml.jackson.core.JsonProcessingException;
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
import com.floragunn.searchsupport.action.Action;
import com.floragunn.searchsupport.action.RestApi;
import com.floragunn.searchsupport.action.StandardRequests.EmptyRequest;
import com.floragunn.searchsupport.action.StandardResponse;
import com.floragunn.searchsupport.xcontent.ObjectTreeXContent;
import com.google.common.collect.ImmutableMap;

public class BulkConfigApi {
    private static final Logger log = LogManager.getLogger(BulkConfigApi.class);

    public static final RestApi REST_API = new RestApi()//
            .handlesGet("/_searchguard/config").with(GetAction.INSTANCE)//
            .handlesPut("/_searchguard/config").with(UpdateAction.INSTANCE)//
            .name("Search Guard Config Management API for retrieving and updating all config types in one batch");

    public static class GetAction extends Action<EmptyRequest, GetAction.Response> {

        public static final GetAction INSTANCE = new GetAction();
        public static final String NAME = "cluster:admin:searchguard:config/bulk/get";

        protected GetAction() {
            super(NAME, EmptyRequest::new, Response::new);
        }

        public static class Response extends Action.Response {

            private Map<String, Object> config;

            public Response() {
            }

            public Response(Map<String, Object> config) {
                this.config = config;
            }

            public Response(UnparsedMessage message) throws ConfigValidationException {
                this.config = message.requiredDocNode().toMap();
            }

            @Override
            public Object toBasicObject() {
                return config;
            }
        }

        public static class Handler extends Action.Handler<EmptyRequest, Response> {

            private final ConfigurationRepository configurationRepository;

            private final static ToXContent.Params OMIT_DEFAULTS_PARAMS = new ToXContent.MapParams(ImmutableMap.of("omit_defaults", "true"));

            @Inject
            public Handler(HandlerDependencies handlerDependencies, ConfigurationRepository configurationRepository) {
                super(GetAction.INSTANCE, handlerDependencies);

                this.configurationRepository = configurationRepository;
            }

            @Override
            protected CompletableFuture<Response> doExecute(EmptyRequest request) {
                return CompletableFuture.supplyAsync(() -> {
                    Map<CType, SgDynamicConfiguration<?>> configMap = configurationRepository
                            .getConfigurationsFromIndex(Arrays.asList(CType.values()), true);
                    Map<String, Object> result = new LinkedHashMap<>();

                    for (Map.Entry<CType, SgDynamicConfiguration<?>> entry : configMap.entrySet()) {
                        SgDynamicConfiguration<?> config = entry.getValue();

                        if (config != null) {
                            Map<String, Object> resultEntry = new LinkedHashMap<>();

                            if (config.getUninterpolatedJson() != null) {
                                resultEntry.put("content", UnparsedDoc.fromJson(config.getUninterpolatedJson()));
                            } else {
                                resultEntry.put("content", ObjectTreeXContent.toObjectTree(config, OMIT_DEFAULTS_PARAMS));
                            }

                            resultEntry.put("_version", config.getDocVersion());
                            resultEntry.put("_seq_no", config.getSeqNo());
                            resultEntry.put("_primary_term", config.getPrimaryTerm());

                            result.put(entry.getKey().toLCString(), resultEntry);

                        } else {
                            result.put(entry.getKey().toLCString(), null);
                        }
                    }

                    return new Response(result);
                }, getExecutor());
            }
        }
    }

    public static class UpdateAction extends Action<UpdateAction.Request, StandardResponse> {

        public static final UpdateAction INSTANCE = new UpdateAction();
        public static final String NAME = "cluster:admin:searchguard:config/bulk/update";

        protected UpdateAction() {
            super(NAME, Request::new, StandardResponse::new);
        }

        public static class Request extends Action.Request {

            private final UnparsedDoc<?> config;

            public Request(UnparsedDoc<?> config) {
                super();
                this.config = config;
            }

            public Request(UnparsedMessage message) throws ConfigValidationException {
                this.config = message.requiredUnparsedDoc();
            }

            public UnparsedDoc<?> getConfig() {
                return config;
            }

            @Override
            public Object toBasicObject() {
                return config;
            }
        }

        public static class Handler extends Action.Handler<Request, StandardResponse> {

            private final ConfigurationRepository configurationRepository;

            @Inject
            public Handler(HandlerDependencies handlerDependencies, ConfigurationRepository configurationRepository) {
                super(UpdateAction.INSTANCE, handlerDependencies);

                this.configurationRepository = configurationRepository;
            }

            @Override
            protected final CompletableFuture<StandardResponse> doExecute(Request request) {
                return CompletableFuture.supplyAsync(() -> {
                    try {
                        this.configurationRepository.update(parseConfigJson(request.getConfig()));
                        return new StandardResponse(200).message("Configuration has been updated");
                    } catch (ConfigValidationException e) {
                        return new StandardResponse(400).error(e);
                    } catch (ConfigUpdateException e) {
                        log.error("Error while updating configuration", e);
                        return new StandardResponse(500).error(null, e.getMessage(), e.getDetailsAsMap());
                    }
                });
            }

            private Map<CType, Map<String, Object>> parseConfigJson(UnparsedDoc<?> unparsedDoc) throws ConfigValidationException {
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

}
