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

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.injection.guice.Inject;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.patch.DocPatch;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.searchguard.BaseDependencies;
import com.floragunn.searchguard.configuration.CType;
import com.floragunn.searchguard.configuration.ConcurrentConfigUpdateException;
import com.floragunn.searchguard.configuration.ConfigUpdateException;
import com.floragunn.searchguard.configuration.ConfigurationLoader;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.configuration.NoSuchConfigEntryException;
import com.floragunn.searchguard.configuration.SgConfigEntry;
import com.floragunn.searchsupport.action.Action;
import com.floragunn.searchsupport.action.StandardRequests.IdRequest;
import com.floragunn.searchsupport.action.StandardResponse;

public abstract class DocumentLevelConfigApi {
    private static final Logger log = LogManager.getLogger(DocumentLevelConfigApi.class);

    public static class GetAction extends Action<IdRequest, StandardResponse> {

        protected GetAction(String name) {
            super(name, IdRequest::new, StandardResponse::new);
        }

        public static abstract class Handler<T> extends Action.Handler<IdRequest, StandardResponse> {

            private final ConfigurationRepository configurationRepository;
            private final ConfigurationLoader configLoader;
            private final CType<T> configType;

            @Inject
            public Handler(Action<IdRequest, StandardResponse> action, CType<T> configType, HandlerDependencies handlerDependencies,
                    ConfigurationRepository configurationRepository, BaseDependencies baseDependencies) {
                super(action, handlerDependencies);

                this.configType = configType;
                this.configurationRepository = configurationRepository;
                this.configLoader = new ConfigurationLoader(baseDependencies.getLocalClient(), configurationRepository);
            }

            @Override
            protected CompletableFuture<StandardResponse> doExecute(IdRequest request) {
                return supplyAsync(() -> {
                    try {
                        SgConfigEntry<T> entry = this.configLoader.loadEntrySync(configType, request.getId(), "GET API Request", configurationRepository.getParserContext());

                        return new StandardResponse(200).data(entry.toRedactedBasicObject()).eTag(entry.getETag());
                    } catch (NoSuchConfigEntryException e) {
                        log.info(e.getMessage());
                        return new StandardResponse(404).error(e.getMessage());
                    } catch (Exception e) {
                        log.error("Error while getting user", e);
                        return new StandardResponse(500).error(e.getMessage());
                    }
                });
            }

        }
    }

    public static class PutAction extends Action<PutAction.Request, StandardResponse> {

        protected PutAction(String name) {
            super(name, Request::new, StandardResponse::new);
        }

        public static class Request extends Action.Request {

            private final String id;
            private final Map<String, Object> config;

            public Request(String id, Map<String, Object> config) {
                super();
                this.id = id;
                this.config = config;
            }

            public Request(UnparsedMessage message) throws ConfigValidationException {
                DocNode docNode = message.requiredDocNode();
                this.id = docNode.getAsString("id");
                this.config = docNode.getAsNode("config").toMap();
            }

            public Map<String, Object> getConfig() {
                return config;
            }

            @Override
            public Object toBasicObject() {
                return ImmutableMap.of("id", id, "config", config);
            }

            public String getId() {
                return id;
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
                        ConfigurationRepository.Context context = configurationRepository.getParserContext()
                                .withExternalResources().withoutLenientValidation();
                        T entry = configType.getParser().parse(DocNode.wrap(request.getConfig()), context).get();

                        this.configurationRepository.addOrUpdate(configType, request.getId(), entry, request.getIfMatch());

                        try {
                            if (entry instanceof AutoCloseable) {
                                ((AutoCloseable) entry).close();
                            }
                        } catch (Exception e) {
                            log.warn("Error while closing {}", entry, e);
                        }

                        return new StandardResponse(200).message("Configuration has been updated");
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

    public static class PatchAction extends Action<PatchAction.Request, StandardResponse> {

        protected PatchAction(String name) {
            super(name, PatchAction.Request::new, StandardResponse::new);
        }

        public static class Request extends Action.Request {
            private final String id;
            private final DocPatch patch;

            public Request(String id, DocPatch patch) {
                super();
                this.id = id;
                this.patch = patch;
            }

            public Request(UnparsedMessage message) throws ConfigValidationException {
                super(message);
                ValidationErrors validationErrors = new ValidationErrors();
                ValidatingDocNode vNode = new ValidatingDocNode(message.requiredDocNode(), validationErrors);
                this.id = vNode.get("id").required().asString();
                this.patch = vNode.get("patch").required().by(DocPatch::parseTyped);
            }

            @Override
            public Object toBasicObject() {
                return ImmutableMap.of("id", id, "patch", patch);
            }

            public DocPatch getPatch() {
                return patch;
            }

            public String getId() {
                return id;
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
                return supplyAsync(() -> {
                    try {
                        return this.configRepository.applyPatch(configType, request.getId(), request.getPatch(), request.getIfMatch(),
                                ConfigurationRepository.PatchDefaultHandling.FAIL_ON_MISSING_DOCUMENT);
                    } catch (ConfigValidationException e) {
                        return new StandardResponse(400).error(null, e.getMessage(), e.getValidationErrors());
                    } catch (ConcurrentConfigUpdateException e) {
                        return new StandardResponse(412).error(e.getMessage());
                    } catch (ConfigUpdateException e) {
                        log.error("Error while adding user", e);
                        return new StandardResponse(500).error(null, e.getMessage(), e.getDetailsAsMap());
                    } catch (Exception e) {
                        log.error("Error while adding user", e);
                        return new StandardResponse(500).error(e.getMessage());
                    }
                });
            }
        }
    }
}
