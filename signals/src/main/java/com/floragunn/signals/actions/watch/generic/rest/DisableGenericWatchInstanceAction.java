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
import com.floragunn.searchguard.SearchGuardVersion;
import com.floragunn.searchsupport.action.RestApi;
import com.floragunn.searchsupport.action.StandardResponse;
import com.floragunn.searchsupport.action.Action;
import com.floragunn.signals.Signals;
import com.floragunn.signals.actions.watch.generic.service.GenericWatchService;
import com.floragunn.signals.actions.watch.generic.service.GenericWatchServiceFactory;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.threadpool.ThreadPool;

public class DisableGenericWatchInstanceAction extends Action<DisableGenericWatchInstanceAction.DisableGenericWatchInstanceRequest, StandardResponse> {

    public final static String NAME = "cluster:admin:searchguard:tenant:signals:watch/instances/disable";
    public static final DisableGenericWatchInstanceAction INSTANCE = new DisableGenericWatchInstanceAction();

    public static final RestApi REST_API = new RestApi().responseHeaders(SearchGuardVersion.header())//
        .handlesDelete("/_signals/watch/{tenant}/{id}/instances/{instance_id}/_active")//
        .with(INSTANCE, (params, body) -> new DisableGenericWatchInstanceRequest(params.get("tenant"), params.get("id"), params.get("instance_id")))//
        .name("DELETE /_signals/watch/{tenant}/{id}/instances/{instance_id}/_active");

    public DisableGenericWatchInstanceAction() {
        super(NAME, DisableGenericWatchInstanceRequest::new, StandardResponse::new);
    }

    public static class DisableGenericWatchInstanceHandler extends StandardSyncOperationHandler<DisableGenericWatchInstanceRequest> {

        private final GenericWatchService genericWatchService;

        @Inject
        public DisableGenericWatchInstanceHandler(HandlerDependencies handlerDependencies, Signals signals, Client client,
            ThreadPool threadPool) {
            super(INSTANCE, handlerDependencies, threadPool);
            this.genericWatchService = new GenericWatchServiceFactory(signals, client).create();
        }

        @Override
        protected StandardResponse synchronousExecute(DisableGenericWatchInstanceRequest request) {
            return genericWatchService.switchEnabledFlag(request.getTenantId(),
                request.getWatchId(),
                request.getInstanceId(),
                false);
        }
    }

    public static class DisableGenericWatchInstanceRequest extends Request {

        private final WatchInstanceIdRepresentation id;

        public DisableGenericWatchInstanceRequest(String tenantId, String watchId, String instanceId) {
            this.id = new WatchInstanceIdRepresentation(tenantId, watchId, instanceId);
        }

        public DisableGenericWatchInstanceRequest(UnparsedMessage unparsedMessage) throws ConfigValidationException {
            DocNode docNode = unparsedMessage.requiredDocNode();
            this.id = new WatchInstanceIdRepresentation(docNode);
        }

        @Override
        public Object toBasicObject() {
            return this.id.toBasicObject();
        }

        public String getTenantId() {
            return id.getTenantId();
        }

        public String getWatchId() {
            return id.getWatchId();
        }

        public String getInstanceId() {
            return id.getInstanceId();
        }
    }
}