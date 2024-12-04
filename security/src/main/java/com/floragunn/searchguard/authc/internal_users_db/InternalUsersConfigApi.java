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

package com.floragunn.searchguard.authc.internal_users_db;

import java.util.concurrent.CompletableFuture;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.injection.guice.Inject;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Document;
import com.floragunn.codova.documents.patch.DocPatch;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.searchguard.BaseDependencies;
import com.floragunn.searchguard.configuration.CType;
import com.floragunn.searchguard.configuration.ConcurrentConfigUpdateException;
import com.floragunn.searchguard.configuration.ConfigUpdateException;
import com.floragunn.searchguard.configuration.ConfigurationLoader;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.configuration.NoSuchConfigEntryException;
import com.floragunn.searchguard.configuration.SgConfigEntry;
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
            .with(PutAction.INSTANCE, (params, body) -> new PutAction.Request(params.get("id"), InternalUser.check(body.parseAsDocNode())))
            .handlesPatch("/_searchguard/internal_users/{id}")
            .with(PatchAction.INSTANCE, (params, body) -> new PatchAction.Request(params.get("id"), DocPatch.parse(body)))
            .name("Search Guard Config Management API for retrieving and updating entries in the internal user database");

    public static class GetAction extends Action<StandardRequests.IdRequest, StandardResponse> {
        protected final static Logger log = LogManager.getLogger(GetAction.class);

        public static final GetAction INSTANCE = new GetAction();
        public static final String NAME = "cluster:admin:searchguard:config/internal_users/get";

        protected GetAction() {
            super(NAME, StandardRequests.IdRequest::new, StandardResponse::new);
        }

        public static class Handler extends Action.Handler<StandardRequests.IdRequest, StandardResponse> {

            private final ConfigurationRepository configRepository;
            private final ConfigurationLoader configLoader;

            @Inject
            public Handler(Action.HandlerDependencies handlerDependencies, ConfigurationRepository configurationRepository, BaseDependencies baseDependencies) {
                super(GetAction.INSTANCE, handlerDependencies);
                this.configRepository = configurationRepository;
                this.configLoader = new ConfigurationLoader(baseDependencies.getLocalClient(), configurationRepository);
            }

            @Override
            protected CompletableFuture<StandardResponse> doExecute(StandardRequests.IdRequest request) {
                return supplyAsync(() -> {
                    try {
                        SgConfigEntry<InternalUser> user = this.configLoader.loadEntrySync(CType.INTERNALUSERS, request.getId(),
                                "API GET /_searchguard/internal_users/", this.configRepository.getParserContext());
                        return new StandardResponse(200).data(user.toRedactedBasicObject()).eTag(user.getETag());
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
        protected final static Logger log = LogManager.getLogger(PutAction.class);

        public static final PutAction INSTANCE = new PutAction();
        public static final String NAME = "cluster:admin:searchguard:config/internal_users/put";

        protected PutAction() {
            super(NAME, PutAction.Request::new, StandardResponse::new);
        }

        public static class Request extends Action.Request {
            private final String id;
            private final Document<InternalUser> value;

            public Request(String id, Document<InternalUser> value) {
                super();
                this.id = id;
                this.value = value;
            }

            public Request(UnparsedMessage message) throws ConfigValidationException {
                super(message);
                DocNode docNode = message.requiredDocNode();
                this.id = docNode.getAsString("id");
                this.value = Document.assertedType(docNode.getAsNode("value"), InternalUser.class);
            }

            @Override
            public Object toBasicObject() {
                return ImmutableMap.of("id", id, "value", value);
            }

            public String getId() {
                return id;
            }

            public Document<InternalUser> getValue() {
                return value;
            }
        }

        public static class Handler extends Action.Handler<PutAction.Request, StandardResponse> {

            private final ConfigurationRepository configRepository;

            @Inject
            public Handler(HandlerDependencies handlerDependencies, ConfigurationRepository configRepository) {
                super(PutAction.INSTANCE, handlerDependencies);
                this.configRepository = configRepository;
            }

            @Override
            protected CompletableFuture<StandardResponse> doExecute(PutAction.Request request) {
                return supplyAsync(() -> {
                    try {
                        InternalUser internalUser = InternalUser.parse(request.getValue().toDocNode(), configRepository.getParserContext()
                                .withoutLenientValidation()).get();

                        return this.configRepository.addOrUpdate(CType.INTERNALUSERS, request.getId(), internalUser, request.getIfMatch());
                    } catch (ConfigValidationException e) {
                        return new StandardResponse(400).error(e);
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

    public static class DeleteAction extends Action<StandardRequests.IdRequest, StandardResponse> {
        protected final static Logger log = LogManager.getLogger(DeleteAction.class);

        public static final DeleteAction INSTANCE = new DeleteAction();
        public static final String NAME = "cluster:admin:searchguard:config/internal_users/delete";

        protected DeleteAction() {
            super(NAME, StandardRequests.IdRequest::new, StandardResponse::new);
        }

        public static class Handler extends Action.Handler<StandardRequests.IdRequest, StandardResponse> {

            private final ConfigurationRepository configRepository;

            @Inject
            public Handler(Action.HandlerDependencies handlerDependencies, ConfigurationRepository configurationRepository) {
                super(DeleteAction.INSTANCE, handlerDependencies);
                this.configRepository = configurationRepository;

            }

            @Override
            protected CompletableFuture<StandardResponse> doExecute(StandardRequests.IdRequest request) {
                return supplyAsync(() -> {
                    try {
                        return this.configRepository.delete(CType.INTERNALUSERS, request.getId());
                    } catch (NoSuchConfigEntryException e) {
                        return new StandardResponse(404).error(e.getMessage());
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

    public static class PatchAction extends Action<PatchAction.Request, StandardResponse> {
        protected final static Logger log = LogManager.getLogger(PatchAction.class);

        public static final PatchAction INSTANCE = new PatchAction();
        public static final String NAME = "cluster:admin:searchguard:config/internal_users/patch";

        protected PatchAction() {
            super(NAME, PatchAction.Request::new, StandardResponse::new);
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

            public String getId() {
                return id;
            }

            public DocPatch getPatch() {
                return patch;
            }
        }

        public static class Handler extends Action.Handler<PatchAction.Request, StandardResponse> {

            private final ConfigurationRepository configRepository;

            @Inject
            public Handler(HandlerDependencies handlerDependencies, ConfigurationRepository configRepository) {
                super(PatchAction.INSTANCE, handlerDependencies);
                this.configRepository = configRepository;
            }

            @Override
            protected CompletableFuture<StandardResponse> doExecute(PatchAction.Request request) {
                return supplyAsync(() -> {
                    try {
                        return this.configRepository.applyPatch(CType.INTERNALUSERS, request.getId(), request.getPatch(), request.getIfMatch(),
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
