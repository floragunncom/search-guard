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

package com.floragunn.searchguard.configuration.variables;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.injection.guice.Inject;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.UnparsedDocument;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.searchsupport.action.Action;
import com.floragunn.searchsupport.action.RestApi;
import com.floragunn.searchsupport.action.StandardRequests;
import com.floragunn.searchsupport.action.StandardRequests.EmptyRequest;
import com.floragunn.searchsupport.action.StandardRequests.IdRequest;
import com.floragunn.searchsupport.action.StandardResponse;

public class ConfigVarApi {
    public static final RestApi REST_API = new RestApi()//
            .handlesGet("/_searchguard/config/vars").with(GetAllAction.INSTANCE)//
            .handlesGet("/_searchguard/config/vars/{id}").with(GetAction.INSTANCE, (params, body) -> new IdRequest(params.get("id")))
            .handlesPut("/_searchguard/config/vars").with(UpdateAllAction.INSTANCE)//
            .handlesPut("/_searchguard/config/vars/{id}").with(UpdateAction.INSTANCE, (params, body) -> new UpdateAction.Request(params.get("id"), body))//
            .handlesDelete("/_searchguard/config/vars/{id}").with(DeleteAction.INSTANCE, (params, body) -> new IdRequest(params.get("id")))
            .name("/_searchguard/config/vars");

    public static class GetAction extends Action<StandardRequests.IdRequest, StandardResponse> {
        protected final static Logger log = LogManager.getLogger(GetAction.class);

        public static final GetAction INSTANCE = new GetAction();
        public static final String NAME = "cluster:admin:searchguard:config/vars/get";

        protected GetAction() {
            super(NAME, StandardRequests.IdRequest::new, StandardResponse::new);
        }

        public static class Handler extends Action.Handler<StandardRequests.IdRequest, StandardResponse> {

            private ConfigVarService configVarService;

            @Inject
            public Handler(HandlerDependencies handlerDependencies, ConfigVarService configVarService) {
                super(GetAction.INSTANCE, handlerDependencies);

                this.configVarService = configVarService;

            }

            @Override
            protected CompletableFuture<StandardResponse> doExecute(StandardRequests.IdRequest request) {
                return supplyAsync(() -> {
                    try {
                        ConfigVar value = configVarService.getFromIndex(request.getId());

                        if (value != null) {
                            return new StandardResponse(200).data(value.toBasicObject());
                        } else {
                            return new StandardResponse(404).error("Not found");
                        }
                    } catch (Exception e) {
                        log.error("Error in GetAction", e);
                        return new StandardResponse(500).error(e.getMessage());
                    }
                });

            }
        }
    }

    public static class UpdateAction extends Action<UpdateAction.Request, StandardResponse> {
        protected final static Logger log = LogManager.getLogger(UpdateAction.class);

        public static final UpdateAction INSTANCE = new UpdateAction();
        public static final String NAME = "cluster:admin:searchguard:config/vars/put";

        protected UpdateAction() {
            super(NAME, Request::new, StandardResponse::new);
        }

        public static class Request extends Action.Request {
            private final String id;
            private final Object value;
            private final String scope;
            private final boolean encrypt;

            public Request(String id, Object value, String scope, boolean encrypt) {
                super();
                this.id = id;
                this.value = value;
                this.scope = scope;
                this.encrypt = encrypt;
            }

            public Request(UnparsedMessage message) throws ConfigValidationException {
                super(message);
                DocNode docNode = message.requiredDocNode();
                this.id = docNode.getAsString("id");
                this.value = docNode.get("value");
                this.scope = docNode.getAsString("scope");
                this.encrypt = docNode.getBoolean("encrypt");
            }

            public Request(String id, UnparsedDocument<?> doc) throws ConfigValidationException {
                ValidationErrors validationErrors = new ValidationErrors();
                ValidatingDocNode vNode = new ValidatingDocNode(doc.parseAsDocNode(), validationErrors);
                this.id = id;
                this.value = vNode.get("value").required().asAnything();
                this.scope = vNode.get("scope").asString();
                this.encrypt = vNode.get("encrypt").withDefault(false).asBoolean();
            }

            @Override
            public Object toBasicObject() {
                return ImmutableMap.ofNonNull("id", id, "value", value, "scope", scope, "encrypt", encrypt);
            }

            public String getId() {
                return id;
            }

            public Object getValue() {
                return value;
            }

            public String getScope() {
                return scope;
            }

            public boolean isEncrypt() {
                return encrypt;
            }
        }

        public static class Handler extends Action.Handler<Request, StandardResponse> {

            private ConfigVarService configVarService;

            @Inject
            public Handler(HandlerDependencies handlerDependencies, ConfigVarService configVarService) {
                super(UpdateAction.INSTANCE, handlerDependencies);

                this.configVarService = configVarService;
            }

            @Override
            protected CompletableFuture<StandardResponse> doExecute(Request request) {
                try {
                    return configVarService.update(request);
                } catch (EncryptionException e) {
                    log.error("Error while encrypting data: " + request, e);
                    return CompletableFuture.completedFuture(new StandardResponse(500, e.getMessage()));
                }
            }
        }
    }

    public static class DeleteAction extends Action<IdRequest, StandardResponse> {
        protected final static Logger log = LogManager.getLogger(DeleteAction.class);

        public static final DeleteAction INSTANCE = new DeleteAction();
        public static final String NAME = "cluster:admin:searchguard:config/vars/delete";

        protected DeleteAction() {
            super(NAME, IdRequest::new, StandardResponse::new);
        }

        public static class Handler extends Action.Handler<IdRequest, StandardResponse> {

            private ConfigVarService configVarService;

            @Inject
            public Handler(HandlerDependencies handlerDependencies, ConfigVarService configVarService) {
                super(DeleteAction.INSTANCE, handlerDependencies);

                this.configVarService = configVarService;

            }

            @Override
            protected CompletableFuture<StandardResponse> doExecute(IdRequest request) {
                return configVarService.delete(request.getId());
            }
        }
    }

    public static class GetAllAction extends Action<EmptyRequest, StandardResponse> {
        protected final static Logger log = LogManager.getLogger(GetAllAction.class);

        public static final GetAllAction INSTANCE = new GetAllAction();
        public static final String NAME = "cluster:admin:searchguard:config/vars/get/all";

        protected GetAllAction() {
            super(NAME, EmptyRequest::new, StandardResponse::new);
        }

        public static class Handler extends Action.Handler<EmptyRequest, StandardResponse> {

            private ConfigVarService configVarService;

            @Inject
            public Handler(HandlerDependencies handlerDependencies, ConfigVarService configVarService) {
                super(GetAllAction.INSTANCE, handlerDependencies);

                this.configVarService = configVarService;

            }

            @Override
            protected CompletableFuture<StandardResponse> doExecute(EmptyRequest request) {
                return supplyAsync(() -> {
                    try {
                        return new StandardResponse(200).data(configVarService.getAllFromIndex());
                    } catch (Exception e) {
                        log.error("Error in GetAllAction", e);
                        return new StandardResponse(500).error(e.getMessage());
                    }
                });
            }
        }
    }

    public static class UpdateAllAction extends Action<UpdateAllAction.Request, StandardResponse> {
        protected final static Logger log = LogManager.getLogger(UpdateAllAction.class);

        public static final UpdateAllAction INSTANCE = new UpdateAllAction();
        public static final String NAME = "cluster:admin:searchguard:config/vars/put/all";

        protected UpdateAllAction() {
            super(NAME, Request::new, StandardResponse::new);
        }

        public static class Request extends Action.Request {
            private Map<String, ConfigVar> idToValueMap;

            public Request(Map<String, ConfigVar> idToValueMap) {
                super();
                this.idToValueMap = idToValueMap;
            }

            public Request(UnparsedMessage message) throws ConfigValidationException {
                super(message);
                Map<String, Object> map = message.requiredDocNode().toMap();
                ValidationErrors validationErrors = new ValidationErrors();

                this.idToValueMap = new HashMap<>(map.size());

                for (Map.Entry<String, Object> entry : map.entrySet()) {
                    try {
                        this.idToValueMap.put(entry.getKey(), new ConfigVar(DocNode.wrap(entry.getValue())));
                    } catch (ConfigValidationException e) {
                        validationErrors.add(entry.getKey(), e);
                    }
                }
            }

            @Override
            public Object toBasicObject() {
                return idToValueMap;
            }

            public Map<String, ConfigVar> getIdToValueMap() {
                return idToValueMap;
            }

        }

        public static class Handler extends Action.Handler<Request, StandardResponse> {

            private ConfigVarService configVarService;

            @Inject
            public Handler(HandlerDependencies handlerDependencies, ConfigVarService configVarService) {
                super(UpdateAllAction.INSTANCE, handlerDependencies);

                this.configVarService = configVarService;

            }

            @Override
            protected CompletableFuture<StandardResponse> doExecute(Request request) {
                return configVarService.updateAll(request.getIdToValueMap());
            }
        }
    }

}
