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

package com.floragunn.searchguard.configuration.api;

import java.util.concurrent.CompletableFuture;

import org.elasticsearch.injection.guice.Inject;

import com.floragunn.searchguard.BaseDependencies;
import com.floragunn.searchguard.SearchGuardVersion;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchsupport.action.Action;
import com.floragunn.searchsupport.action.RestApi;
import com.floragunn.searchsupport.action.StandardRequests.EmptyRequest;
import com.floragunn.searchsupport.action.StandardResponse;

public class MigrateConfigIndexApi extends Action<EmptyRequest, StandardResponse> {

    public static final MigrateConfigIndexApi INSTANCE = new MigrateConfigIndexApi();
    public static final String NAME = "cluster:admin:searchguard:config/migrate_index";

    public static final RestApi REST_API = new RestApi()//
            .responseHeaders(SearchGuardVersion.header())//
            .handlesPost("/_searchguard/config/migrate_index").with(MigrateConfigIndexApi.INSTANCE)//
            .name("/_searchguard/config/migrate_index");

    protected MigrateConfigIndexApi() {
        super(NAME, EmptyRequest::new, StandardResponse::new);
    }

    public static class Handler extends Action.Handler<EmptyRequest, StandardResponse> {

        private final ConfigurationRepository configurationRepository;

        @Inject
        public Handler(HandlerDependencies handlerDependencies, BaseDependencies baseDependencies) {
            super(MigrateConfigIndexApi.INSTANCE, handlerDependencies);

            this.configurationRepository = baseDependencies.getConfigurationRepository();
        }

        @Override
        protected final CompletableFuture<StandardResponse> doExecute(EmptyRequest request) {
            return supplyAsync(() -> configurationRepository.migrateIndex());
        }
    }

}
