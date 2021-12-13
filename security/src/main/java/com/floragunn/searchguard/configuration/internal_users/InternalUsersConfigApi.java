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

package com.floragunn.searchguard.configuration.internal_users;

import java.util.concurrent.CompletableFuture;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.inject.Inject;

import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.searchguard.configuration.ConfigUpdateException;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchsupport.action.Action;
import com.floragunn.searchsupport.action.RestApi;
import com.floragunn.searchsupport.action.StandardRequests;
import com.floragunn.searchsupport.action.StandardResponse;
import com.google.common.collect.ImmutableMap;

public class InternalUsersConfigApi {

    public static final RestApi REST_API = new RestApi().handlesGet("/_searchguard/internal_users/{id}")
            .with(InternalUsersConfigApi.GetAction.INSTANCE, (params, body) -> new StandardRequests.IdRequest(params.get("id")))
            .handlesDelete("/_searchguard/internal_users/{id}")
            .with(InternalUsersConfigApi.DeleteAction.INSTANCE, (params, body) -> new StandardRequests.IdRequest(params.get("id")))
            .handlesPut("/_searchguard/internal_users/{id}")
            .with(PutAction.INSTANCE, (params, body) -> new PutAction.Request(params.get("id"), InternalUser.parse(body)))
            .name("Search Guard Config Management API for retrieving and updating entries in the internal user database");

    public static class GetAction extends Action<StandardRequests.IdRequest, StandardResponse> {
        protected final static Logger log = LogManager.getLogger(GetAction.class);

        public static final GetAction INSTANCE = new GetAction();
        public static final String NAME = "cluster:admin:searchguard:config/internal_users/get";

        protected GetAction() {
            super(NAME, StandardRequests.IdRequest::new, StandardResponse::new);
        }

        public static class Handler extends Action.Handler<StandardRequests.IdRequest, StandardResponse> {

            private final InternalUsersService internalUsersService;

            @Inject
            public Handler(Action.HandlerDependencies handlerDependencies, ConfigurationRepository configurationRepository) {
                super(GetAction.INSTANCE, handlerDependencies);
                this.internalUsersService = new InternalUsersService(configurationRepository);
            }

            @Override
            protected CompletableFuture<StandardResponse> doExecute(StandardRequests.IdRequest request) {
                return CompletableFuture.supplyAsync(() -> {
                    try {
                        InternalUser user = this.internalUsersService.getUser(request.getId());
                        return new StandardResponse(200).data(user.toRedactedBasicObject());
                    } catch (InternalUserNotFoundException e) {
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
        protected final static Logger log = LogManager.getLogger(PutAction.class);

        public static final PutAction INSTANCE = new PutAction();
        public static final String NAME = "cluster:admin:searchguard:config/internal_users/put";

        protected PutAction() {
            super(NAME, PutAction.Request::new, StandardResponse::new);
        }

        public static class Request extends Action.Request {
            private final String id;
            private final InternalUser value;

            public Request(String id, InternalUser value) {
                super();
                this.id = id;
                this.value = value;
            }

            public Request(UnparsedMessage message) throws ConfigValidationException {
                super(message);
                ValidationErrors validationErrors = new ValidationErrors();
                ValidatingDocNode vNode = new ValidatingDocNode(message.requiredDocNode(), validationErrors);
                this.id = vNode.get("id").required().asString();
                this.value = vNode.get("value").required().by(InternalUser::parse);
            }

            @Override
            public Object toBasicObject() {
                return ImmutableMap.of("id", id, "value", value);
            }

            public String getId() {
                return id;
            }

            public InternalUser getValue() {
                return value;
            }
        }

        public static class Handler extends Action.Handler<PutAction.Request, StandardResponse> {

            private final InternalUsersService internalUsersService;

            @Inject
            public Handler(HandlerDependencies handlerDependencies, ConfigurationRepository configurationRepository) {
                super(PutAction.INSTANCE, handlerDependencies);
                this.internalUsersService = new InternalUsersService(configurationRepository);
            }

            @Override
            protected CompletableFuture<StandardResponse> doExecute(PutAction.Request request) {
                return CompletableFuture.supplyAsync(() -> {
                    try {
                        String user = request.getId();
                        this.internalUsersService.addOrUpdateUser(user, request.getValue());
                        return new StandardResponse(200).message("User " + user + " has been added");
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

    public static class DeleteAction extends Action<StandardRequests.IdRequest, StandardResponse> {
        protected final static Logger log = LogManager.getLogger(DeleteAction.class);

        public static final DeleteAction INSTANCE = new DeleteAction();
        public static final String NAME = "cluster:admin:searchguard:config/internal_users/delete";

        protected DeleteAction() {
            super(NAME, StandardRequests.IdRequest::new, StandardResponse::new);
        }

        public static class Handler extends Action.Handler<StandardRequests.IdRequest, StandardResponse> {

            private final InternalUsersService internalUsersService;

            @Inject
            public Handler(Action.HandlerDependencies handlerDependencies, ConfigurationRepository configurationRepository) {
                super(DeleteAction.INSTANCE, handlerDependencies);
                this.internalUsersService = new InternalUsersService(configurationRepository);

            }

            @Override
            protected CompletableFuture<StandardResponse> doExecute(StandardRequests.IdRequest request) {
                return CompletableFuture.supplyAsync(() -> {
                    String user = request.getId();
                    try {
                        this.internalUsersService.deleteUser(user);
                        return new StandardResponse(204).message("User " + user + " has been deleted");
                    } catch (InternalUserNotFoundException e) {
                        log.info("User {} for deletion not found", user);
                        return new StandardResponse(404).error("User " + user + " for deletion not found");
                    } catch (ConfigUpdateException e) {
                        log.error("Error while deleting user", e);
                        return new StandardResponse(500).error(null, e.getMessage(), e.getDetailsAsMap());
                    } catch (Exception e) {
                        log.error("Error while deleting user", e);
                        return new StandardResponse(500).error(e.getMessage());
                    }
                });
            }
        }
    }
}
