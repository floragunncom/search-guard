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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.inject.Inject;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.searchguard.configuration.ConfigUpdateException;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.sgconf.impl.v7.InternalUserV7;
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
            .with(AddAction.INSTANCE, (params, body) -> new AddAction.Request(params.get("id"), body.toNormalizedMap()))
            .handlesPatch("/_searchguard/internal_users/{id}")
            .with(UpdateAction.INSTANCE, (params, body) -> new UpdateAction.Request(params.get("id"), body.toNormalizedMap()))
            .name("Search Guard Config Management API for retrieving and updating ");

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
                        InternalUserV7 user = this.internalUsersService.getUser(request.getId());
                        HashMap<String, Object> userRepresentation = new HashMap<>();
                        userRepresentation.put("backend_roles", user.getBackend_roles());
                        userRepresentation.put("search_guard_roles", user.getSearch_guard_roles());
                        userRepresentation.put("attributes", user.getAttributes());
                        return new StandardResponse(200).message("User found").data(userRepresentation);
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

    public static class AddAction extends Action<AddAction.Request, StandardResponse> {
        protected final static Logger log = LogManager.getLogger(AddAction.class);

        public static final AddAction INSTANCE = new AddAction();
        public static final String NAME = "cluster:admin:searchguard:config/internal_users/add";

        protected AddAction() {
            super(NAME, AddAction.Request::new, StandardResponse::new);
        }

        public static class Request extends Action.Request {
            private final String id;
            private final Map<String, Object> value;

            public Request(String id, Map<String, Object> value) {
                super();
                this.id = id;
                this.value = value;
            }

            public Request(UnparsedMessage message) throws ConfigValidationException {
                super(message);
                DocNode docNode = message.requiredDocNode();
                this.id = docNode.getAsString("id");
                this.value = docNode.toNormalizedMap();
            }

            @Override
            public Object toBasicObject() {
                return ImmutableMap.of("id", id, "value", value);
            }

            public String getId() {
                return id;
            }

            public Map<String, Object> getValue() {
                return value;
            }
        }

        public static class Handler extends Action.Handler<AddAction.Request, StandardResponse> {

            private final InternalUsersService internalUsersService;

            @Inject
            public Handler(HandlerDependencies handlerDependencies, ConfigurationRepository configurationRepository) {
                super(AddAction.INSTANCE, handlerDependencies);
                this.internalUsersService = new InternalUsersService(configurationRepository);
            }

            @Override
            protected CompletableFuture<StandardResponse> doExecute(AddAction.Request request) {
                return CompletableFuture.supplyAsync(() -> {
                    try {
                        String user = request.getId();
                        this.internalUsersService.addUser(user, request.getValue());
                        return new StandardResponse(200).message("User " + user + " has been added");
                    } catch (ConfigValidationException e) {
                        return new StandardResponse(400).error(e);
                    } catch (InternalUserAlreadyExistsException e) {
                        log.info(e.getMessage());
                        return new StandardResponse(422).error(e.getMessage());
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


    public static class UpdateAction extends Action<UpdateAction.Request, StandardResponse> {
        protected final static Logger log = LogManager.getLogger(AddAction.class);

        public static final UpdateAction INSTANCE = new UpdateAction();
        public static final String NAME = "cluster:admin:searchguard:config/internal_users/patch";

        protected UpdateAction() {
            super(NAME, UpdateAction.Request::new, StandardResponse::new);
        }

        public static class Request extends Action.Request {
            private final String id;
            private final Map<String, Object> value;

            public Request(String id, Map<String, Object> value) {
                super();
                this.id = id;
                this.value = value;
            }

            public Request(UnparsedMessage message) throws ConfigValidationException {
                super(message);
                DocNode docNode = message.requiredDocNode();
                this.id = docNode.getAsString("id");
                this.value = docNode.toNormalizedMap();
            }

            @Override
            public Object toBasicObject() {
                return ImmutableMap.of("id", id, "value", value);
            }

            public String getId() {
                return id;
            }

            public Map<String, Object> getValue() {
                return value;
            }
        }

        public static class Handler extends Action.Handler<UpdateAction.Request, StandardResponse> {

            private final InternalUsersService internalUsersService;

            @Inject
            public Handler(HandlerDependencies handlerDependencies, ConfigurationRepository configurationRepository) {
                super(UpdateAction.INSTANCE, handlerDependencies);
                this.internalUsersService = new InternalUsersService(configurationRepository);
            }

            @Override
            protected CompletableFuture<StandardResponse> doExecute(UpdateAction.Request request) {
                return CompletableFuture.supplyAsync(() -> {
                    try {
                        String user = request.getId();
                        this.internalUsersService.updateUser(user, request.getValue());
                        return new StandardResponse(200).message("User " + user + " has been updated");
                    } catch (ConfigValidationException e) {
                        return new StandardResponse(400).error(e);
                    } catch (InternalUserNotFoundException e) {
                        log.info(e.getMessage());
                        return new StandardResponse(404).error(e.getMessage());
                    } catch (ConfigUpdateException e) {
                        log.error("Error while updating user", e);
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
        protected final static Logger log = LogManager.getLogger(GetAction.class);

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
                    } catch (ConfigValidationException e) {
                        return new StandardResponse(400).error(e);
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
