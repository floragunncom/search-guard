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

package com.floragunn.searchguard.configuration.secrets;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.inject.Inject;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.searchsupport.action.Action;
import com.floragunn.searchsupport.action.RestApi;
import com.floragunn.searchsupport.action.StandardRequests;
import com.floragunn.searchsupport.action.StandardRequests.EmptyRequest;
import com.floragunn.searchsupport.action.StandardRequests.IdRequest;
import com.floragunn.searchsupport.action.StandardResponse;
import com.google.common.collect.ImmutableMap;

public class SecretsConfigApi {
    public static final RestApi REST_API = new RestApi()//
            .handlesGet("/_searchguard/secrets").with(GetAllAction.INSTANCE)//
            .handlesGet("/_searchguard/secrets/{id}").with(GetAction.INSTANCE, (params, body) -> new IdRequest(params.get("id")))
            .handlesPut("/_searchguard/secrets").with(UpdateAllAction.INSTANCE)//
            .handlesPut("/_searchguard/secrets/{id}")
            .with(UpdateAction.INSTANCE, (params, body) -> new UpdateAction.Request(params.get("id"), body.toBasicObject()))//
            .handlesDelete("/_searchguard/secrets/{id}").with(DeleteAction.INSTANCE, (params, body) -> new IdRequest(params.get("id")))
            .name("Search Guard Secrets");

    public static class GetAction extends Action<StandardRequests.IdRequest, StandardResponse> {
        protected final static Logger log = LogManager.getLogger(GetAction.class);

        public static final GetAction INSTANCE = new GetAction();
        public static final String NAME = "cluster:admin:searchguard:config/secret/get";

        protected GetAction() {
            super(NAME, StandardRequests.IdRequest::new, StandardResponse::new);
        }

        public static class Handler extends Action.Handler<StandardRequests.IdRequest, StandardResponse> {

            private SecretsService secretsService;

            @Inject
            public Handler(HandlerDependencies handlerDependencies, SecretsService secretsService) {
                super(GetAction.INSTANCE, handlerDependencies);

                this.secretsService = secretsService;

            }

            @Override
            protected CompletableFuture<StandardResponse> doExecute(StandardRequests.IdRequest request) {
                Object value = secretsService.get(request.getId());

                if (value != null) {
                    return CompletableFuture.completedFuture(new StandardResponse(200).data(value));
                } else {
                    return CompletableFuture.completedFuture(new StandardResponse(404).error("Not found"));
                }
            }
        }
    }

    public static class UpdateAction extends Action<UpdateAction.Request, StandardResponse> {
        protected final static Logger log = LogManager.getLogger(UpdateAction.class);

        public static final UpdateAction INSTANCE = new UpdateAction();
        public static final String NAME = "cluster:admin:searchguard:config/secret/update";

        protected UpdateAction() {
            super(NAME, Request::new, StandardResponse::new);
        }

        public static class Request extends Action.Request {
            private final String id;
            private final Object value;

            public Request(String id, Object value) {
                super();
                this.id = id;
                this.value = value;
            }

            public Request(UnparsedMessage message) throws ConfigValidationException {
                super(message);
                DocNode docNode = message.requiredDocNode();
                this.id = docNode.getAsString("id");
                this.value = docNode.get("value");
            }

            @Override
            public Object toBasicObject() {
                return ImmutableMap.of("id", id, "value", value);
            }

            public String getId() {
                return id;
            }

            public Object getValue() {
                return value;
            }
        }

        public static class Handler extends Action.Handler<Request, StandardResponse> {

            private SecretsService secretsService;

            @Inject
            public Handler(HandlerDependencies handlerDependencies, SecretsService secretsService) {
                super(UpdateAction.INSTANCE, handlerDependencies);

                this.secretsService = secretsService;

            }

            @Override
            protected CompletableFuture<StandardResponse> doExecute(Request request) {
                return secretsService.update(request.getId(), request.getValue());
            }
        }
    }

    public static class DeleteAction extends Action<IdRequest, StandardResponse> {
        protected final static Logger log = LogManager.getLogger(DeleteAction.class);

        public static final DeleteAction INSTANCE = new DeleteAction();
        public static final String NAME = "cluster:admin:searchguard:config/secret/delete";

        protected DeleteAction() {
            super(NAME, IdRequest::new, StandardResponse::new);
        }

        public static class Handler extends Action.Handler<IdRequest, StandardResponse> {

            private SecretsService secretsService;

            @Inject
            public Handler(HandlerDependencies handlerDependencies, SecretsService secretsService) {
                super(DeleteAction.INSTANCE, handlerDependencies);

                this.secretsService = secretsService;

            }

            @Override
            protected CompletableFuture<StandardResponse> doExecute(IdRequest request) {
                return secretsService.delete(request.getId());
            }
        }
    }

    public static class GetAllAction extends Action<EmptyRequest, StandardResponse> {
        protected final static Logger log = LogManager.getLogger(GetAllAction.class);

        public static final GetAllAction INSTANCE = new GetAllAction();
        public static final String NAME = "cluster:admin:searchguard:config/secret/get/all";

        protected GetAllAction() {
            super(NAME, EmptyRequest::new, StandardResponse::new);
        }

        public static class Handler extends Action.Handler<EmptyRequest, StandardResponse> {

            private SecretsService secretsService;

            @Inject
            public Handler(HandlerDependencies handlerDependencies, SecretsService secretsService) {
                super(GetAllAction.INSTANCE, handlerDependencies);

                this.secretsService = secretsService;

            }

            @Override
            protected CompletableFuture<StandardResponse> doExecute(EmptyRequest request) {
                return CompletableFuture.completedFuture(new StandardResponse(200).data(secretsService.getAll()));
            }

        }
    }

    public static class UpdateAllAction extends Action<UpdateAllAction.Request, StandardResponse> {
        protected final static Logger log = LogManager.getLogger(UpdateAllAction.class);

        public static final UpdateAllAction INSTANCE = new UpdateAllAction();
        public static final String NAME = "cluster:admin:searchguard:config/secret/update/all";

        protected UpdateAllAction() {
            super(NAME, Request::new, StandardResponse::new);
        }

        public static class Request extends Action.Request {
            private Map<String, Object> idToValueMap;

            public Request(Map<String, Object> idToValueMap) {
                super();
                this.idToValueMap = idToValueMap;
            }

            public Request(UnparsedMessage message) throws ConfigValidationException {
                super(message);
                this.idToValueMap = message.requiredDocNode().toMap();
            }

            @Override
            public Object toBasicObject() {
                return idToValueMap;
            }

            public Map<String, Object> getIdToValueMap() {
                return idToValueMap;
            }

        }

        public static class Handler extends Action.Handler<Request, StandardResponse> {

            private SecretsService secretsService;

            @Inject
            public Handler(HandlerDependencies handlerDependencies, SecretsService secretsService) {
                super(UpdateAllAction.INSTANCE, handlerDependencies);

                this.secretsService = secretsService;

            }

            @Override
            protected CompletableFuture<StandardResponse> doExecute(Request request) {
                return secretsService.updateAll(request.getIdToValueMap());
            }
        }
    }

}
