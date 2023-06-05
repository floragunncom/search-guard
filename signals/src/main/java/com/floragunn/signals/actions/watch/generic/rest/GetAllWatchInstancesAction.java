/*
 * Copyright 2019-2023 floragunn GmbH
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
package com.floragunn.signals.actions.watch.generic.rest;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.searchguard.SearchGuardVersion;
import com.floragunn.searchsupport.action.RestApi;
import com.floragunn.searchsupport.action.StandardResponse;
import com.floragunn.searchsupport.action.Action;
import com.floragunn.signals.Signals;
import com.floragunn.signals.actions.watch.generic.service.GenericWatchService;
import com.floragunn.signals.actions.watch.generic.service.GenericWatchServiceFactory;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import static com.floragunn.signals.actions.watch.generic.rest.WatchInstanceIdRepresentation.FIELD_TENANT_ID;
import static com.floragunn.signals.actions.watch.generic.rest.WatchInstanceIdRepresentation.FIELD_WATCH_ID;

public class GetAllWatchInstancesAction extends Action<GetAllWatchInstancesAction.GetAllWatchInstancesRequest, StandardResponse> {

    public final static String NAME = "cluster:admin:searchguard:tenant:signals:watch/get_all_instances";
    public static final GetAllWatchInstancesAction INSTANCE = new GetAllWatchInstancesAction();

    public static final RestApi REST_API = new RestApi().responseHeaders(SearchGuardVersion.header())//
        .handlesGet("/_signals/watch/{tenant}/{id}/instances")//
        .with(INSTANCE, (params, body) -> new GetAllWatchInstancesRequest(params.get("tenant"), params.get("id")))//
        .name("GET /_signals/watch/{tenant}/{id}/instances");

    public GetAllWatchInstancesAction() {
        super(NAME, GetAllWatchInstancesRequest::new, StandardResponse::new);
    }


    public static class GetAllWatchInstancesHandler extends Handler<GetAllWatchInstancesRequest, StandardResponse> {

        private final GenericWatchService genericWatchService;

        @Inject
        public GetAllWatchInstancesHandler(HandlerDependencies dependencies, Signals signals, Client client) {
            super(INSTANCE, dependencies);
            this.genericWatchService = new GenericWatchServiceFactory(signals, client).create();
        }

        @Override
        protected CompletableFuture<StandardResponse> doExecute(GetAllWatchInstancesRequest request) {
            return supplyAsync(() -> genericWatchService.findAllInstances(request));
        }
    }

    public static class GetAllWatchInstancesRequest extends Request {

        private final String tenantId;
        private final String watchId;

        public GetAllWatchInstancesRequest(String tenantId, String watchId) {
            this.tenantId = Objects.requireNonNull(tenantId, "Tenant id is required");
            this.watchId = Objects.requireNonNull(watchId, "Watch id is required");
        }

        public GetAllWatchInstancesRequest(UnparsedMessage unparsedMessage) throws ConfigValidationException {
            DocNode docNode = unparsedMessage.requiredDocNode();
            this.tenantId = docNode.getAsString(FIELD_TENANT_ID);
            this.watchId = docNode.getAsString(FIELD_WATCH_ID);
        }

        @Override
        public ImmutableMap<String, String> toBasicObject() {
            return ImmutableMap.of(FIELD_TENANT_ID, tenantId, FIELD_WATCH_ID, watchId);
        }

        public String getTenantId() {
            return tenantId;
        }

        public String getWatchId() {
            return watchId;
        }
    }
}