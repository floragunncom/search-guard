/*
  * Copyright 2022 by floragunn GmbH - All rights reserved
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed here is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 * This software is free of charge for non-commercial and academic use.
 * For commercial use in a production environment you have to obtain a license
 * from https://floragunn.com
 *
 */
package com.floragunn.searchguard.authtoken;

import com.floragunn.codova.documents.patch.DocPatch;
import com.floragunn.searchguard.BaseDependencies;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.configuration.api.TypeLevelConfigApi;
import com.floragunn.searchsupport.action.RestApi;
import com.google.common.collect.ImmutableList;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.plugins.ActionPlugin.ActionHandler;

public class AuthTokenServiceConfigApi extends TypeLevelConfigApi {

    public static final RestApi REST_API = new RestApi()//
            .handlesGet("/_searchguard/config/auth_token_service").with(GetAction.INSTANCE)//
            .handlesPut("/_searchguard/config/auth_token_service")
            .with(PutAction.INSTANCE, (params, body) -> new PutAction.Request(body.parseAsMap()))//
            .handlesPatch("/_searchguard/config/auth_token_service")
            .with(PatchAction.INSTANCE, (params, body) -> new PatchAction.Request(DocPatch.parse(body)))
            .name("/_searchguard/config/auth_token_service");

    public static final ImmutableList<ActionHandler<?, ?>> ACTION_HANDLERS = ImmutableList.of(
            new ActionHandler<>(AuthTokenServiceConfigApi.GetAction.INSTANCE, AuthTokenServiceConfigApi.GetAction.Handler.class),
            new ActionHandler<>(AuthTokenServiceConfigApi.PutAction.INSTANCE, AuthTokenServiceConfigApi.PutAction.Handler.class),
            new ActionHandler<>(AuthTokenServiceConfigApi.PatchAction.INSTANCE, AuthTokenServiceConfigApi.PatchAction.Handler.class));

    public static class GetAction extends TypeLevelConfigApi.GetAction {
        public static final GetAction INSTANCE = new GetAction();
        public static final String NAME = "cluster:admin:searchguard:config/auth_token_service/get";

        public GetAction() {
            super(NAME);
        }

        public static class Handler extends TypeLevelConfigApi.GetAction.Handler<AuthTokenServiceConfig> {
            @Inject
            public Handler(HandlerDependencies handlerDependencies, ConfigurationRepository configurationRepository,
                    BaseDependencies baseDependencies) {
                super(INSTANCE, AuthTokenServiceConfig.TYPE, handlerDependencies, configurationRepository, baseDependencies);
            }
        }

    }

    public static class PutAction extends TypeLevelConfigApi.PutAction {
        public static final PutAction INSTANCE = new PutAction();
        public static final String NAME = "cluster:admin:searchguard:config/auth_token_service/put";

        public PutAction() {
            super(NAME);
        }

        public static class Handler extends TypeLevelConfigApi.PutAction.Handler<AuthTokenServiceConfig> {
            @Inject
            public Handler(HandlerDependencies handlerDependencies, ConfigurationRepository configurationRepository) {
                super(INSTANCE, AuthTokenServiceConfig.TYPE, handlerDependencies, configurationRepository);
            }
        }
    }

    public static class PatchAction extends TypeLevelConfigApi.PatchAction {
        public static final PatchAction INSTANCE = new PatchAction();
        public static final String NAME = "cluster:admin:searchguard:config/auth_token_service/patch";

        public PatchAction() {
            super(NAME);
        }

        public static class Handler extends TypeLevelConfigApi.PatchAction.Handler<AuthTokenServiceConfig> {
            @Inject
            public Handler(HandlerDependencies handlerDependencies, ConfigurationRepository configurationRepository) {
                super(INSTANCE, AuthTokenServiceConfig.TYPE, handlerDependencies, configurationRepository);
            }
        }
    }
}
