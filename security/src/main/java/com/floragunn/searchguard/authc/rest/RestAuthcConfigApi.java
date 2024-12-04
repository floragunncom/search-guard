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

package com.floragunn.searchguard.authc.rest;

import com.floragunn.searchguard.configuration.ConcurrentConfigUpdateException;
import com.floragunn.searchguard.configuration.ConfigUpdateException;
import com.floragunn.searchsupport.action.Action;
import com.floragunn.searchsupport.action.StandardRequests;
import com.floragunn.searchsupport.action.StandardResponse;
import org.elasticsearch.injection.guice.Inject;

import com.floragunn.codova.documents.patch.DocPatch;
import com.floragunn.searchguard.BaseDependencies;
import com.floragunn.searchguard.configuration.CType;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.configuration.api.TypeLevelConfigApi;
import com.floragunn.searchsupport.action.RestApi;

import java.util.concurrent.CompletableFuture;

public class RestAuthcConfigApi extends TypeLevelConfigApi {

    public static final RestApi REST_API = new RestApi()//
            .handlesGet("/_searchguard/config/authc").with(GetAction.INSTANCE)//
            .handlesPut("/_searchguard/config/authc").with(PutAction.INSTANCE, (params, body) -> new PutAction.Request(body.parseAsMap()))//
            .handlesPatch("/_searchguard/config/authc").with(PatchAction.INSTANCE, (params, body) -> new PatchAction.Request(DocPatch.parse(body)))
            .handlesDelete("/_searchguard/config/authc").with(DeleteAction.INSTANCE)
            .name("/_searchguard/config/authc");

    public static class GetAction extends TypeLevelConfigApi.GetAction {
        public static final GetAction INSTANCE = new GetAction();
        public static final String NAME = "cluster:admin:searchguard:config/authc/get";

        public GetAction() {
            super(NAME);
        }

        public static class Handler extends TypeLevelConfigApi.GetAction.Handler<RestAuthcConfig> {
            @Inject
            public Handler(HandlerDependencies handlerDependencies, ConfigurationRepository configurationRepository,
                    BaseDependencies baseDependencies) {
                super(INSTANCE, CType.AUTHC, handlerDependencies, configurationRepository, baseDependencies);
            }
        }

    }

    public static class PutAction extends TypeLevelConfigApi.PutAction {
        public static final PutAction INSTANCE = new PutAction();
        public static final String NAME = "cluster:admin:searchguard:config/authc/put";

        public PutAction() {
            super(NAME);
        }

        public static class Handler extends TypeLevelConfigApi.PutAction.Handler<RestAuthcConfig> {
            @Inject
            public Handler(HandlerDependencies handlerDependencies, ConfigurationRepository configurationRepository) {
                super(INSTANCE, CType.AUTHC, handlerDependencies, configurationRepository);
            }
        }
    }

    public static class PatchAction extends TypeLevelConfigApi.PatchAction {
        public static final PatchAction INSTANCE = new PatchAction();
        public static final String NAME = "cluster:admin:searchguard:config/authc/patch";

        public PatchAction() {
            super(NAME);
        }

        public static class Handler extends TypeLevelConfigApi.PatchAction.Handler<RestAuthcConfig> {
            @Inject
            public Handler(HandlerDependencies handlerDependencies, ConfigurationRepository configurationRepository) {
                super(INSTANCE, CType.AUTHC, handlerDependencies, configurationRepository);
            }
        }
    }

    public static class DeleteAction extends Action<StandardRequests.EmptyRequest, StandardResponse> {
        public static final String NAME = "cluster:admin:searchguard:config/authc/delete";
        public static final DeleteAction INSTANCE = new DeleteAction();

        private DeleteAction() {
            super(NAME, StandardRequests.EmptyRequest::new, StandardResponse::new);
        }

        public static class Handler extends Action.Handler<StandardRequests.EmptyRequest, StandardResponse> {
            private final ConfigurationRepository configurationRepository;

            @Inject
            public Handler(HandlerDependencies handlerDependencies, ConfigurationRepository configurationRepository) {
                super(INSTANCE, handlerDependencies);
                this.configurationRepository = configurationRepository;
            }

            @Override
            protected CompletableFuture<StandardResponse> doExecute(StandardRequests.EmptyRequest request) {
                return supplyAsync(() -> {
                    try {
                        return configurationRepository.delete(CType.AUTHC);
                    } catch (ConfigUpdateException | ConcurrentConfigUpdateException e) {
                        return new StandardResponse(e);
                    }
                });
            }
        }
    }
}