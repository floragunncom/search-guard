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

package com.floragunn.searchguard.enterprise.femt;

import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.searchguard.enterprise.femt.tenants.GetAvailableTenantsAction;
import com.floragunn.searchsupport.action.StandardRequests;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.plugins.ActionPlugin.ActionHandler;

import com.floragunn.codova.documents.patch.DocPatch;
import com.floragunn.searchguard.BaseDependencies;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.configuration.api.TypeLevelConfigApi;
import com.floragunn.searchsupport.action.RestApi;

public class FeMultiTenancyConfigApi extends TypeLevelConfigApi {

    public static final RestApi REST_API = new RestApi()//
            .handlesGet("/_searchguard/config/fe_multi_tenancy").with(GetAction.INSTANCE)//
            .handlesGet("/_searchguard/config/frontend_multi_tenancy").with(GetAction.INSTANCE)//
            .handlesPut("/_searchguard/config/fe_multi_tenancy").with(PutAction.INSTANCE, (params, body) -> new PutAction.Request(body.parseAsMap()))//
            .handlesPut("/_searchguard/config/frontend_multi_tenancy").with(PutAction.INSTANCE, (params, body) -> new PutAction.Request(body.parseAsMap()))//
            .handlesPatch("/_searchguard/config/fe_multi_tenancy").with(PatchAction.INSTANCE, (params, body) -> new PatchAction.Request(DocPatch.parse(body)))
            .handlesPatch("/_searchguard/config/frontend_multi_tenancy").with(PatchAction.INSTANCE, (params, body) -> new PatchAction.Request(DocPatch.parse(body)))
            .handlesGet("/_searchguard/current_user/tenants").with(GetAvailableTenantsAction.INSTANCE, (params, body) -> new StandardRequests.EmptyRequest())//
            .name("/_searchguard/config/frontend_multi_tenancy");

    public static final ImmutableList<ActionHandler<?, ?>> ACTION_HANDLERS = ImmutableList.of(
            new ActionHandler<>(FeMultiTenancyConfigApi.GetAction.INSTANCE, FeMultiTenancyConfigApi.GetAction.Handler.class),
            new ActionHandler<>(FeMultiTenancyConfigApi.PutAction.INSTANCE, FeMultiTenancyConfigApi.PutAction.Handler.class),
            new ActionHandler<>(FeMultiTenancyConfigApi.PatchAction.INSTANCE, FeMultiTenancyConfigApi.PatchAction.Handler.class),
            new ActionHandler<>(GetAvailableTenantsAction.INSTANCE, GetAvailableTenantsAction.GetAvailableTenantsHandler.class)
        );

    public static class GetAction extends TypeLevelConfigApi.GetAction {
        public static final GetAction INSTANCE = new GetAction();
        public static final String NAME = "cluster:admin:searchguard:config/fe_multi_tenancy/get";

        public GetAction() {
            super(NAME);
        }

        public static class Handler extends TypeLevelConfigApi.GetAction.Handler<FeMultiTenancyConfig> {
            @Inject
            public Handler(HandlerDependencies handlerDependencies, ConfigurationRepository configurationRepository,
                    BaseDependencies baseDependencies) {
                super(INSTANCE, FeMultiTenancyConfig.TYPE, handlerDependencies, configurationRepository, baseDependencies);
            }
        }

    }

    public static class PutAction extends TypeLevelConfigApi.PutAction {
        public static final PutAction INSTANCE = new PutAction();
        public static final String NAME = "cluster:admin:searchguard:config/fe_multi_tenancy/put";

        public PutAction() {
            super(NAME);
        }

        public static class Handler extends TypeLevelConfigApi.PutAction.Handler<FeMultiTenancyConfig> {
            @Inject
            public Handler(HandlerDependencies handlerDependencies, ConfigurationRepository configurationRepository) {
                super(INSTANCE, FeMultiTenancyConfig.TYPE, handlerDependencies, configurationRepository);
            }
        }
    }

    public static class PatchAction extends TypeLevelConfigApi.PatchAction {
        public static final PatchAction INSTANCE = new PatchAction();
        public static final String NAME = "cluster:admin:searchguard:config/fe_multi_tenancy/patch";

        public PatchAction() {
            super(NAME);
        }

        public static class Handler extends TypeLevelConfigApi.PatchAction.Handler<FeMultiTenancyConfig> {
            @Inject
            public Handler(HandlerDependencies handlerDependencies, ConfigurationRepository configurationRepository) {
                super(INSTANCE, FeMultiTenancyConfig.TYPE, handlerDependencies, configurationRepository);
            }
        }
    }
}