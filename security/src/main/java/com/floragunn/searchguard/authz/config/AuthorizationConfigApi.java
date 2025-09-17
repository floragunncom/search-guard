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

package com.floragunn.searchguard.authz.config;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.plugins.ActionPlugin.ActionHandler;

import com.floragunn.codova.documents.patch.DocPatch;
import com.floragunn.searchguard.BaseDependencies;
import com.floragunn.searchguard.configuration.CType;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.configuration.api.TypeLevelConfigApi;
import com.floragunn.searchsupport.action.RestApi;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.floragunn.searchsupport.action.ActionHandlerFactory.actionHandler;

public class AuthorizationConfigApi extends TypeLevelConfigApi {

    public static final RestApi REST_API = new RestApi()//
            .handlesGet("/_searchguard/config/authz").with(GetAction.INSTANCE)//
            .handlesPut("/_searchguard/config/authz").with(PutAction.INSTANCE, (params, body) -> new PutAction.Request(body.parseAsMap()))//
            .handlesPatch("/_searchguard/config/authz").with(PatchAction.INSTANCE, (params, body) -> new PatchAction.Request(DocPatch.parse(body)))
            .name("/_searchguard/config/authz");

    public static final List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> ACTION_HANDLERS = List.of(
            actionHandler(AuthorizationConfigApi.GetAction.INSTANCE, AuthorizationConfigApi.GetAction.Handler.class),
            actionHandler(AuthorizationConfigApi.PutAction.INSTANCE, AuthorizationConfigApi.PutAction.Handler.class),
            actionHandler(AuthorizationConfigApi.PatchAction.INSTANCE, AuthorizationConfigApi.PatchAction.Handler.class));

    public static class GetAction extends TypeLevelConfigApi.GetAction {
        public static final GetAction INSTANCE = new GetAction();
        public static final String NAME = "cluster:admin:searchguard:config/authz/get";

        public GetAction() {
            super(NAME);
        }

        public static class Handler extends TypeLevelConfigApi.GetAction.Handler<AuthorizationConfig> {
            @Inject
            public Handler(HandlerDependencies handlerDependencies, ConfigurationRepository configurationRepository,
                    BaseDependencies baseDependencies) {
                super(INSTANCE, CType.AUTHZ, handlerDependencies, configurationRepository, baseDependencies);
            }
        }

    }

    public static class PutAction extends TypeLevelConfigApi.PutAction {
        public static final PutAction INSTANCE = new PutAction();
        public static final String NAME = "cluster:admin:searchguard:config/authz/put";

        public PutAction() {
            super(NAME);
        }

        public static class Handler extends TypeLevelConfigApi.PutAction.Handler<AuthorizationConfig> {
            @Inject
            public Handler(HandlerDependencies handlerDependencies, ConfigurationRepository configurationRepository) {
                super(INSTANCE, CType.AUTHZ, handlerDependencies, configurationRepository);
            }
        }
    }

    public static class PatchAction extends TypeLevelConfigApi.PatchAction {
        public static final PatchAction INSTANCE = new PatchAction();
        public static final String NAME = "cluster:admin:searchguard:config/authz/patch";

        public PatchAction() {
            super(NAME);
        }

        public static class Handler extends TypeLevelConfigApi.PatchAction.Handler<AuthorizationConfig> {
            @Inject
            public Handler(HandlerDependencies handlerDependencies, ConfigurationRepository configurationRepository) {
                super(INSTANCE, CType.AUTHZ, handlerDependencies, configurationRepository);
            }
        }
    }
}