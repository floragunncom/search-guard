/*
 * Copyright 2021-2022 floragunn GmbH
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

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Strings;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.xcontent.ToXContent;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.DocUtils;
import com.floragunn.codova.documents.DocumentParseException;
import com.floragunn.codova.documents.Format;
import com.floragunn.codova.documents.UnparsedDocument;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.errors.InvalidAttributeValue;
import com.floragunn.codova.validation.errors.MissingAttribute;
import com.floragunn.codova.validation.errors.ValidationError;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.searchguard.BaseDependencies;
import com.floragunn.searchguard.SearchGuardVersion;
import com.floragunn.searchguard.auditlog.AuditLog;
import com.floragunn.searchguard.configuration.CType;
import com.floragunn.searchguard.configuration.ConcurrentConfigUpdateException;
import com.floragunn.searchguard.configuration.ConfigMap;
import com.floragunn.searchguard.configuration.ConfigUnavailableException;
import com.floragunn.searchguard.configuration.ConfigUpdateException;
import com.floragunn.searchguard.configuration.ConfigurationLoader;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.configuration.ConfigurationRepository.ConfigUpdateResult;
import com.floragunn.searchguard.configuration.ConfigurationRepository.ConfigWithMetadata;
import com.floragunn.searchguard.configuration.SgDynamicConfiguration;
import com.floragunn.searchguard.configuration.variables.ConfigVar;
import com.floragunn.searchguard.configuration.variables.ConfigVarService;
import com.floragunn.searchsupport.action.Action;
import com.floragunn.searchsupport.action.RestApi;
import com.floragunn.searchsupport.action.StandardRequests.EmptyRequest;
import com.floragunn.searchsupport.action.StandardResponse;
import com.floragunn.searchsupport.xcontent.ObjectTreeXContent;

public class BulkConfigApi {
    private static final Logger log = LogManager.getLogger(BulkConfigApi.class);

    public static final RestApi REST_API = new RestApi()//
            .responseHeaders(SearchGuardVersion.header())//
            .handlesGet("/_searchguard/config").with(GetAction.INSTANCE)//
            .handlesPut("/_searchguard/config").with(UpdateAction.INSTANCE)//
            .name("/_searchguard/config");

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
            private final ConfigVarService configVarService;
            private final ConfigurationLoader configLoader;
            private final AuditLog auditLog;

            private final static ToXContent.Params OMIT_DEFAULTS_PARAMS = new ToXContent.MapParams(ImmutableMap.of("omit_defaults", "true"));

            @Inject
            public Handler(HandlerDependencies handlerDependencies, ConfigurationRepository configurationRepository,
                    ConfigVarService configVarService, BaseDependencies baseDependencies) {
                super(GetAction.INSTANCE, handlerDependencies);

                this.configurationRepository = configurationRepository;
                this.configVarService = configVarService;
                this.auditLog = baseDependencies.getAuditLog();
                this.configLoader = new ConfigurationLoader(baseDependencies.getLocalClient(), configurationRepository);
            }

            @Override
            protected CompletableFuture<Response> doExecute(EmptyRequest request) {
                return supplyAsync(() -> {
                    try {
                        ConfigMap configMap = this.configLoader.loadSync(CType.all(), "GET Bulk API Request",
                                this.configurationRepository.getParserContext());

                        logComplianceEvent(configMap);

                        Map<String, Object> result = new LinkedHashMap<>();

                        for (SgDynamicConfiguration<?> config : configMap.getAll()) {
                            Map<String, Object> resultEntry = new LinkedHashMap<>();

                            if (config.getUninterpolatedJson() != null) {
                                if (config.getCType().getArity() == CType.Arity.SINGLE) {
                                    try {
                                        DocNode parsedConfig = DocNode.parse(Format.JSON).from(config.getUninterpolatedJson());

                                        if (parsedConfig.hasNonNull("default")) {
                                            resultEntry.put("content", parsedConfig.get("default"));
                                        }
                                    } catch (DocumentParseException e) {
                                        throw new ConfigUnavailableException(e);
                                    }
                                } else {
                                    resultEntry.put("content", UnparsedDocument.fromJson(config.getUninterpolatedJson()));
                                }
                            } else {
                                resultEntry.put("content", ObjectTreeXContent.toObjectTree(config, OMIT_DEFAULTS_PARAMS));
                            }

                            if (config.documentExists()) {
                                resultEntry.put("exists", true);
                                resultEntry.put("_version", config.getDocVersion());
                                resultEntry.put("_seq_no", config.getSeqNo());
                                resultEntry.put("_primary_term", config.getPrimaryTerm());
                                resultEntry.put("_etag", config.getETag());
                            } else {
                                resultEntry.put("exists", false);
                            }

                            result.put(config.getCType().toLCString(), resultEntry);
                        }

                        result.put("config_vars", ImmutableMap.of("content", configVarService.getAllFromIndex()));

                        return new Response(result);
                    } catch (ConfigUnavailableException e) {
                        throw new CompletionException(e);
                    }
                });
            }

            private void logComplianceEvent(ConfigMap configMap) {
                Map<String, String> fields = new LinkedHashMap<>();

                for (SgDynamicConfiguration<?> config : configMap.getAll()) {
                    fields.put(config.getCType().toLCString(), Strings.toString(config));
                }

                auditLog.logDocumentRead(configurationRepository.getEffectiveSearchGuardIndex(), "*", null, fields);
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

            private final UnparsedDocument<?> config;

            public Request(UnparsedDocument<?> config) {
                super();
                this.config = config;
            }

            public Request(UnparsedMessage message) throws ConfigValidationException {
                this.config = message.requiredUnparsedDoc();
            }

            public UnparsedDocument<?> getConfig() {
                return config;
            }

            @Override
            public Object toBasicObject() {
                return config;
            }
        }

        public static class Handler extends Action.Handler<Request, StandardResponse> {

            private final ConfigurationRepository configurationRepository;
            private final ConfigVarService configVarService;

            @Inject
            public Handler(HandlerDependencies handlerDependencies, ConfigurationRepository configurationRepository,
                    ConfigVarService configVarService) {
                super(UpdateAction.INSTANCE, handlerDependencies);

                this.configurationRepository = configurationRepository;
                this.configVarService = configVarService;
            }

            @Override
            protected final CompletableFuture<StandardResponse> doExecute(Request request) {
                return supplyAsync(() -> {
                    try {
                        Map<CType<?>, ConfigWithMetadata> configMap = parseConfigJson(request.getConfig());

                        ConfigWithMetadata configVars = configMap.get(CType.CONFIG_VARS);

                        if (configVars != null && configVars.getContent() != null) {
                            configMap.remove(CType.CONFIG_VARS);

                            try {
                                StandardResponse response = this.configVarService.updateAll((Map<String, ConfigVar>) configVars.getContent()).get();

                                if (response.getStatus() >= 300) {
                                    return response;
                                }
                            } catch (InterruptedException e) {
                                log.error("Error while updating configuration", e);
                                return new StandardResponse(500).error(e.getMessage());
                            } catch (ExecutionException e) {
                                if (e.getCause() instanceof ConfigValidationException) {
                                    return new StandardResponse(400).error((ConfigValidationException) e.getCause());
                                } else {
                                    log.error("Error while updating configuration", e);
                                    return new StandardResponse(500).error(e.getMessage());
                                }
                            }
                        }

                        if (!configMap.isEmpty()) {
                            Map<CType<?>, ConfigUpdateResult> result = this.configurationRepository.update(configMap);
                            return new StandardResponse(200).message("Configuration has been updated").data(result);
                        } else {
                            return new StandardResponse(200).message("No configuration was provided");
                        }
                    } catch (ConfigValidationException e) {
                        return new StandardResponse(400).error(e);
                    } catch (ConcurrentConfigUpdateException e) {
                        return new StandardResponse(412).error(e.getMessage()).data(e.getUpdateResult());
                    } catch (ConfigUpdateException e) {
                        log.error("Error while updating configuration", e);
                        return new StandardResponse(500).error(null, e.getMessage(), e.getDetailsAsMap()).data(e.getUpdateResult());
                    }
                });
            }

            private Map<CType<?>, ConfigWithMetadata> parseConfigJson(UnparsedDocument<?> unparsedDoc) throws ConfigValidationException {
                Map<String, Object> parsedJson = unparsedDoc.parseAsMap();
                ValidationErrors validationErrors = new ValidationErrors();
                Map<CType<?>, ConfigWithMetadata> configTypeToConfigMap = new HashMap<>();

                for (String configTypeName : parsedJson.keySet()) {
                    CType<?> ctype;

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

                    Map<String, ?> contentMap = DocUtils.toStringKeyedMap((Map<?, ?>) content);

                    //request cannot contain static entries
                    for (Map.Entry<String, ?> entry : contentMap.entrySet()) {
                        DocNode configEntry = DocNode.wrap(entry.getValue());
                        Object staticField = configEntry.get("static");
                        if (Boolean.TRUE.equals(staticField)) {
                            validationErrors.add(new InvalidAttributeValue(configTypeName + ".content." + entry.getKey(), entry.getValue(), "Non-static entry"));
                        }
                    }

                    if (ctype == CType.CONFIG_VARS) {
                        Map<String, ConfigVar> parsedContentMap = new LinkedHashMap<>(contentMap.size());

                        for (Map.Entry<String, ?> entry : contentMap.entrySet()) {
                            try {
                                parsedContentMap.put(entry.getKey(), new ConfigVar(DocNode.wrap(entry.getValue())));
                            } catch (ConfigValidationException e) {
                                validationErrors.add(configTypeName + ".content." + entry.getKey(), e);
                            }
                        }

                        contentMap = parsedContentMap;
                    }

                    String etag = ((Map<?, ?>) value).get("etag") != null ? String.valueOf(((Map<?, ?>) value).get("etag")) : null;

                    configTypeToConfigMap.put(ctype, new ConfigWithMetadata(contentMap, etag));
                }

                validationErrors.throwExceptionForPresentErrors();

                return configTypeToConfigMap;
            }
        }
    }
}
