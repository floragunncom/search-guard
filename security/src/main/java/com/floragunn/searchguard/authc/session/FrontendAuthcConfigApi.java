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

package com.floragunn.searchguard.authc.session;

import org.elasticsearch.injection.guice.Inject;

import com.floragunn.codova.documents.patch.DocPatch;
import com.floragunn.searchguard.BaseDependencies;
import com.floragunn.searchguard.configuration.CType;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.configuration.api.DocumentLevelConfigApi;
import com.floragunn.searchguard.configuration.api.TypeLevelConfigApi;
import com.floragunn.searchsupport.action.RestApi;
import com.floragunn.searchsupport.action.StandardRequests;

public class FrontendAuthcConfigApi {

    public static class TypeLevel extends TypeLevelConfigApi {

        public static final RestApi REST_API = new RestApi()//
                .handlesGet("/_searchguard/config/authc_frontend").with(GetAction.INSTANCE)//
                .handlesPut("/_searchguard/config/authc_frontend")
                .with(PutAction.INSTANCE, (params, body) -> new PutAction.Request(body.parseAsMap()))//
                .handlesPatch("/_searchguard/config/authc_frontend")
                .with(PatchAction.INSTANCE, (params, body) -> new PatchAction.Request(DocPatch.parse(body)))
                .name("/_searchguard/config/authc_frontend");

        public static class GetAction extends TypeLevelConfigApi.GetAction {
            public static final GetAction INSTANCE = new GetAction();
            public static final String NAME = "cluster:admin:searchguard:config/authc_frontend/_all/get";

            public GetAction() {
                super(NAME);
            }

            public static class Handler extends TypeLevelConfigApi.GetAction.Handler<FrontendAuthcConfig> {
                @Inject
                public Handler(HandlerDependencies handlerDependencies, ConfigurationRepository configurationRepository,
                        BaseDependencies baseDependencies) {
                    super(INSTANCE, CType.FRONTEND_AUTHC, handlerDependencies, configurationRepository, baseDependencies);
                }
            }

        }

        public static class PutAction extends TypeLevelConfigApi.PutAction {
            public static final PutAction INSTANCE = new PutAction();
            public static final String NAME = "cluster:admin:searchguard:config/authc_frontend/_all/put";

            public PutAction() {
                super(NAME);
            }

            public static class Handler extends TypeLevelConfigApi.PutAction.Handler<FrontendAuthcConfig> {
                @Inject
                public Handler(HandlerDependencies handlerDependencies, ConfigurationRepository configurationRepository) {
                    super(INSTANCE, CType.FRONTEND_AUTHC, handlerDependencies, configurationRepository);
                }
            }
        }

        public static class PatchAction extends TypeLevelConfigApi.PatchAction {
            public static final PatchAction INSTANCE = new PatchAction();
            public static final String NAME = "cluster:admin:searchguard:config/authc_frontend/_all/patch";

            public PatchAction() {
                super(NAME);
            }

            public static class Handler extends TypeLevelConfigApi.PatchAction.Handler<FrontendAuthcConfig> {
                @Inject
                public Handler(HandlerDependencies handlerDependencies, ConfigurationRepository configurationRepository) {
                    super(INSTANCE, CType.FRONTEND_AUTHC, handlerDependencies, configurationRepository);
                }
            }
        }
    }

    public static class DocumentLevel extends DocumentLevelConfigApi {

        public static final RestApi REST_API = new RestApi()//
                .handlesGet("/_searchguard/config/authc_frontend/{id}")
                .with(GetAction.INSTANCE, (params, body) -> new StandardRequests.IdRequest(params.get("id")))//
                .handlesPut("/_searchguard/config/authc_frontend/{id}")
                .with(PutAction.INSTANCE, (params, body) -> new PutAction.Request(params.get("id"), body.parseAsMap()))//
                .handlesPatch("/_searchguard/config/authc_frontend/{id}")
                .with(PatchAction.INSTANCE, (params, body) -> new PatchAction.Request(params.get("id"), DocPatch.parse(body)))
                .name("/_searchguard/config/authc_frontend/{id}");

        public static class GetAction extends DocumentLevelConfigApi.GetAction {
            public static final GetAction INSTANCE = new GetAction();
            public static final String NAME = "cluster:admin:searchguard:config/authc_frontend/_id/get";

            public GetAction() {
                super(NAME);
            }

            public static class Handler extends DocumentLevelConfigApi.GetAction.Handler<FrontendAuthcConfig> {
                @Inject
                public Handler(HandlerDependencies handlerDependencies, ConfigurationRepository configurationRepository,
                        BaseDependencies baseDependencies) {
                    super(INSTANCE, CType.FRONTEND_AUTHC, handlerDependencies, configurationRepository, baseDependencies);
                }
            }

        }

        public static class PutAction extends DocumentLevelConfigApi.PutAction {
            public static final PutAction INSTANCE = new PutAction();
            public static final String NAME = "cluster:admin:searchguard:config/authc_frontend/_id/put";

            public PutAction() {
                super(NAME);
            }

            public static class Handler extends DocumentLevelConfigApi.PutAction.Handler<FrontendAuthcConfig> {
                @Inject
                public Handler(HandlerDependencies handlerDependencies, ConfigurationRepository configurationRepository) {
                    super(INSTANCE, CType.FRONTEND_AUTHC, handlerDependencies, configurationRepository);
                }
            }
        }

        public static class PatchAction extends DocumentLevelConfigApi.PatchAction {
            public static final PatchAction INSTANCE = new PatchAction();
            public static final String NAME = "cluster:admin:searchguard:config/authc_frontend/_id/patch";

            public PatchAction() {
                super(NAME);
            }

            public static class Handler extends DocumentLevelConfigApi.PatchAction.Handler<FrontendAuthcConfig> {
                @Inject
                public Handler(HandlerDependencies handlerDependencies, ConfigurationRepository configurationRepository) {
                    super(INSTANCE, CType.FRONTEND_AUTHC, handlerDependencies, configurationRepository);
                }
            }
        }
    }
}