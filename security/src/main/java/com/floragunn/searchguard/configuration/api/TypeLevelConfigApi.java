/*
 * Copyright 2022 floragunn GmbH
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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import com.floragunn.searchguard.configuration.CType;
import com.floragunn.searchguard.configuration.ConcurrentConfigUpdateException;
import com.floragunn.searchguard.configuration.ConfigUnavailableException;
import com.floragunn.searchguard.configuration.ConfigUpdateException;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.configuration.ConfigurationRepository.Context;
import com.floragunn.searchguard.configuration.SgDynamicConfiguration;
import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Strings;
import org.elasticsearch.injection.guice.Inject;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.DocumentParseException;
import com.floragunn.codova.documents.Format;
import com.floragunn.codova.documents.UnparsedDocument;
import com.floragunn.codova.documents.patch.DocPatch;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.searchguard.BaseDependencies;
import com.floragunn.searchguard.auditlog.AuditLog;
import com.floragunn.searchsupport.action.Action;
import com.floragunn.searchsupport.action.StandardRequests.EmptyRequest;
import com.floragunn.searchsupport.action.StandardResponse;

public abstract class TypeLevelConfigApi {
    private static final Logger log = LogManager.getLogger(TypeLevelConfigApi.class);

    public abstract static class GetAction extends Action<EmptyRequest, GetAction.Response> {

        protected GetAction(String name) {
            super(name, EmptyRequest::new, Response::new);
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

        public static abstract class Handler<T> extends Action.Handler<EmptyRequest, Response> {

            private final ConfigurationRepository configurationRepository;
            private final AuditLog auditLog;
            private final CType<T> configType;

            @Inject
            public Handler(Action<EmptyRequest, Response> action, CType<T> configType, HandlerDependencies handlerDependencies,
                    ConfigurationRepository configurationRepository, BaseDependencies baseDependencies) {
                super(action, handlerDependencies);

                this.configType = configType;
                this.configurationRepository = configurationRepository;
                this.auditLog = baseDependencies.getAuditLog();
            }

            @Override
            protected CompletableFuture<Response> doExecute(EmptyRequest request) {
                return supplyAsync(() -> {
                    try {
                        SgDynamicConfiguration<T> config = configurationRepository.getConfigurationFromIndex(configType, "GET API Request");

                        logComplianceEvent(config);

                        if (config.documentExists()) {

                            Map<String, Object> result = new LinkedHashMap<>();

                            if (configType.getArity() == CType.Arity.SINGLE) {
                                try {
                                    DocNode parsedConfig = DocNode.parse(Format.JSON).from(config.getUninterpolatedJson());

                                    if (parsedConfig.hasNonNull("default")) {
                                        result.put("content", parsedConfig.get("default"));
                                    }
                                } catch (DocumentParseException e) {
                                    throw new ConfigUnavailableException(e);
                                }
                            } else {
                                result.put("content", UnparsedDocument.fromJson(config.getUninterpolatedJson()));
                            }

                            result.put("_version", config.getDocVersion());
                            result.put("_seq_no", config.getSeqNo());
                            result.put("_primary_term", config.getPrimaryTerm());

                            return new Response(result);
                        }

                        Response response = new Response();
                        response.status(HttpStatus.SC_NOT_FOUND);

                        return response;
                    } catch (ConfigUnavailableException e) {
                        throw new CompletionException(e);
                    }
                });
            }

            private void logComplianceEvent(SgDynamicConfiguration<?> config) {
                Map<String, String> fields = new LinkedHashMap<>();

                fields.put(config.getCType().toLCString(), Strings.toString(config));

                auditLog.logDocumentRead(configurationRepository.getEffectiveSearchGuardIndex(), configType.getName(), null, fields);
            }
        }
    }

    public abstract static class PutAction extends Action<PutAction.Request, StandardResponse> {

        protected PutAction(String name) {
            super(name, Request::new, StandardResponse::new);
        }

        public static class Request extends Action.Request {

            private final Map<String, Object> config;

            public Request(Map<String, Object> config) {
                super();
                this.config = config;
            }

            public Request(UnparsedMessage message) throws ConfigValidationException {
                DocNode docNode = message.requiredDocNode();
                this.config = docNode.getAsNode("config").toMap();
            }

            public Map<String, Object> getConfig() {
                return config;
            }

            @Override
            public Object toBasicObject() {
                return ImmutableMap.of("config", config);
            }
        }

        public static class Handler<T> extends Action.Handler<Request, StandardResponse> {

            private final ConfigurationRepository configurationRepository;
            private final CType<T> configType;

            @Inject
            public Handler(Action<Request, StandardResponse> action, CType<T> configType, HandlerDependencies handlerDependencies,
                    ConfigurationRepository configurationRepository) {
                super(action, handlerDependencies);

                this.configurationRepository = configurationRepository;
                this.configType = configType;
            }

            @Override
            protected final CompletableFuture<StandardResponse> doExecute(Request request) {
                return supplyAsync(() -> {
                    try {
                        Map<String, Object> configMap;

                        if (configType.getArity() == CType.Arity.SINGLE) {
                            configMap = ImmutableMap.of("default", request.getConfig());
                        } else {
                            configMap = request.getConfig();
                        }

                        Context context = configurationRepository.getParserContext()
                                .withExternalResources().withoutLenientValidation();

                        try (SgDynamicConfiguration<T> config = SgDynamicConfiguration.fromMap(configMap, configType, context).get()) {

                            this.configurationRepository.update(configType, config, request.getIfMatch());
                            return new StandardResponse(200).message("Configuration has been updated");
                        }
                    } catch (ConfigValidationException e) {
                        return new StandardResponse(400).error(e);
                    } catch (ConcurrentConfigUpdateException e) {
                        return new StandardResponse(412).error(e.getMessage());
                    } catch (ConfigUpdateException e) {
                        log.error("Error while updating configuration", e);
                        return new StandardResponse(500).error(null, e.getMessage(), e.getDetailsAsMap());
                    }
                });
            }

        }
    }

    public abstract static class PatchAction extends Action<PatchAction.Request, StandardResponse> {

        protected PatchAction(String name) {
            super(name, PatchAction.Request::new, StandardResponse::new);
        }

        public static class Request extends Action.Request {
            private final DocPatch patch;

            public Request(DocPatch patch) {
                super();
                this.patch = patch;
            }

            public Request(UnparsedMessage message) throws ConfigValidationException {
                super(message);
                ValidationErrors validationErrors = new ValidationErrors();
                ValidatingDocNode vNode = new ValidatingDocNode(message.requiredDocNode(), validationErrors);
                this.patch = vNode.get("patch").required().by(DocPatch::parseTyped);
            }

            @Override
            public Object toBasicObject() {
                return ImmutableMap.of("patch", patch);
            }

            public DocPatch getPatch() {
                return patch;
            }
        }

        public static class Handler<T> extends Action.Handler<PatchAction.Request, StandardResponse> {

            private final ConfigurationRepository configRepository;
            private final CType<T> configType;

            @Inject
            public Handler(Action<Request, StandardResponse> action, CType<T> configType, HandlerDependencies handlerDependencies,
                    ConfigurationRepository configRepository) {
                super(action, handlerDependencies);
                this.configRepository = configRepository;
                this.configType = configType;
            }

            @Override
            protected CompletableFuture<StandardResponse> doExecute(PatchAction.Request request) {
                return CompletableFuture.supplyAsync(() -> {
                    try {
                        return this.configRepository.applyPatch(configType, request.getPatch(), request.getIfMatch());
                    } catch (ConfigValidationException e) {
                        return new StandardResponse(400).error(null, e.getMessage(), e.getValidationErrors());
                    } catch (ConcurrentConfigUpdateException e) {
                        return new StandardResponse(412).error(e.getMessage());
                    } catch (ConfigUpdateException e) {
                        log.error("Error while updating configuration", e);
                        return new StandardResponse(500).error(null, e.getMessage(), e.getDetailsAsMap());
                    } catch (Exception e) {
                        log.error("Error while updating configuration", e);
                        return new StandardResponse(500).error(e.getMessage());
                    }
                }, getExecutor());
            }
        }
    }
}
