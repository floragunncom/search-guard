/*
 * Copyright 2023 floragunn GmbH
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

import com.floragunn.searchguard.SearchGuardVersion;
import com.floragunn.searchguard.configuration.CType;
import com.floragunn.searchguard.configuration.ConcurrentConfigUpdateException;
import com.floragunn.searchguard.configuration.ConfigUpdateException;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchsupport.action.Action;
import com.floragunn.searchsupport.action.RestApi;
import com.floragunn.searchsupport.action.StandardRequests;
import com.floragunn.searchsupport.action.StandardResponse;

public class GenericTypeLevelConfigApi {

    public static final RestApi REST_API = new RestApi()//
            .responseHeaders(SearchGuardVersion.header())//
            .handlesDelete("/_searchguard/config/{config_type}")
            .with(DeleteAction.INSTANCE, (params, body) -> new StandardRequests.IdRequest(params.get("config_type")))
            .name("/_searchguard/config/type");

    public static class DeleteAction extends Action<StandardRequests.IdRequest, StandardResponse> {
        public static final String NAME = "cluster:admin:searchguard:config/delete_by_type";
        public static final DeleteAction INSTANCE = new DeleteAction();

        private DeleteAction() {
            super(NAME, StandardRequests.IdRequest::new, StandardResponse::new);
        }

        public static class Handler extends Action.Handler<StandardRequests.IdRequest, StandardResponse> {
            private final ConfigurationRepository configurationRepository;

            @Inject
            public Handler(HandlerDependencies handlerDependencies, ConfigurationRepository configurationRepository) {
                super(INSTANCE, handlerDependencies);
                this.configurationRepository = configurationRepository;
            }

            @Override
            protected CompletableFuture<StandardResponse> doExecute(StandardRequests.IdRequest request) {
                return supplyAsync(() -> {
                    try {
                        return configurationRepository.delete(CType.valueOf(request.getId()));
                    } catch (IllegalArgumentException | ConfigUpdateException | ConcurrentConfigUpdateException e) {
                        return new StandardResponse(e);
                    }
                });
            }
        }
    }
}
