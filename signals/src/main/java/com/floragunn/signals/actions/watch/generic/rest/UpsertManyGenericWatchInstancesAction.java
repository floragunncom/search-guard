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
import com.floragunn.codova.documents.UnparsedDocument;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.errors.ValidationError;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.SearchGuardVersion;
import com.floragunn.searchsupport.action.RestApi;
import com.floragunn.searchsupport.action.StandardResponse;
import com.floragunn.searchsupport.action.Action;
import com.floragunn.signals.Signals;
import com.floragunn.signals.actions.watch.generic.rest.UpsertOneGenericWatchInstanceAction.UpsertOneGenericWatchInstanceRequest;
import com.floragunn.signals.actions.watch.generic.service.GenericWatchService;
import com.floragunn.signals.actions.watch.generic.service.GenericWatchServiceFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.floragunn.signals.actions.watch.generic.rest.WatchInstanceIdRepresentation.FIELD_TENANT_ID;
import static com.floragunn.signals.actions.watch.generic.rest.WatchInstanceIdRepresentation.FIELD_WATCH_ID;

public class UpsertManyGenericWatchInstancesAction extends Action<UpsertManyGenericWatchInstancesAction.UpsertManyGenericWatchInstancesRequest, StandardResponse> {

    private static final Logger log = LogManager.getLogger(UpsertManyGenericWatchInstancesAction.class);

    public final static String NAME = "cluster:admin:searchguard:tenant:signals:watch/instances/upsert_many";
    public static final UpsertManyGenericWatchInstancesAction INSTANCE = new UpsertManyGenericWatchInstancesAction();

    public static final RestApi REST_API = new RestApi().responseHeaders(SearchGuardVersion.header())//
        .handlesPut("/_signals/watch/{tenant}/{id}/instances")//
        .with(INSTANCE, (params, body) -> new UpsertManyGenericWatchInstancesRequest(params.get("tenant"), params.get("id"), body))//
        .name("PUT /_signals/watch/{tenant}/{id}/instances");

    public UpsertManyGenericWatchInstancesAction() {
        super(NAME, UpsertManyGenericWatchInstancesRequest::new, StandardResponse::new);
    }

    public static class UpsertManyGenericWatchInstancesHandler extends StandardSyncOperationHandler<UpsertManyGenericWatchInstancesRequest> {

        private final GenericWatchService genericWatchService;

        @Inject
        public UpsertManyGenericWatchInstancesHandler(HandlerDependencies handlerDependencies, Signals signals, Client client,
            ThreadPool threadPool) {
            super(INSTANCE, handlerDependencies, threadPool);
            this.genericWatchService = new GenericWatchServiceFactory(signals, client).create();
        }

        @Override
        protected StandardResponse synchronousExecute(UpsertManyGenericWatchInstancesRequest request) {
            try {
                return genericWatchService.upsertManyInstances(request);
            } catch (ConfigValidationException e) {
                log.error("Cannot create generic watch instances because validation errors occured.", e);
                return new StandardResponse(400) //
                    .message("Cannot create generic watch instances because validation errors occurred.") //
                    .error(e);
            }
        }
    }

    public static class UpsertManyGenericWatchInstancesRequest extends Request {

        public static final String WATCH_INSTANCES = "watch_instances";
        private final String tenantId;
        private final String watchId;
        private final ImmutableMap<String, Object> watchInstances;

        public UpsertManyGenericWatchInstancesRequest(String tenantId, String watchId, UnparsedDocument<?> message) throws ConfigValidationException {
            this.tenantId = Objects.requireNonNull(tenantId, "Tenant id is required");
            this.watchId = Objects.requireNonNull(watchId, "Watch id is required");
            if(message == null) {
                ValidationError validationError = new ValidationError("body",
                    "Request body is required and should contains watch instances");
                throw new ConfigValidationException(validationError);
            }
            DocNode docNode = message.parseAsDocNode();
            this.watchInstances = docNode.toMap();
            if(watchInstances.isEmpty()) {
                ValidationError validationError = new ValidationError("body",
                    "Request does not contain any definitions of watch instances.");
                throw new ConfigValidationException(validationError);
            }
        }

        public UpsertManyGenericWatchInstancesRequest(UnparsedMessage unparsedMessage) throws ConfigValidationException {
            DocNode docNode = unparsedMessage.requiredDocNode();
            this.tenantId = docNode.getAsString(FIELD_TENANT_ID);
            this.watchId = docNode.getAsString(FIELD_WATCH_ID);
            DocNode watchInstancesNode = docNode.getAsNode(WATCH_INSTANCES);
            if (watchInstancesNode == null) {
                ValidationError validationError = new ValidationError("body", "Request does not contain any definitions of watch instances.");
                throw new ConfigValidationException(validationError);
            }
            this.watchInstances = docNode.toMap();
            if (watchInstances.isEmpty()) {
                ValidationError validationError = new ValidationError("body", "Request does not contain any definitions of watch instances.");
                throw new ConfigValidationException(validationError);
            }
        }

        public String getTenantId() {
            return tenantId;
        }

        public String getWatchId() {
            return watchId;
        }

        @Override
        public ImmutableMap<String, Object> toBasicObject() {
            return ImmutableMap.of(FIELD_TENANT_ID, tenantId, FIELD_WATCH_ID, watchId, WATCH_INSTANCES, watchInstances);
        }

        public ImmutableList<UpsertOneGenericWatchInstanceRequest> toCreateOneWatchInstanceRequest() {
            List<UpsertOneGenericWatchInstanceRequest> mutableList = watchInstances.entrySet()//
                    .stream()//
                    .map(entry -> new UpsertOneGenericWatchInstanceRequest(tenantId, watchId, entry.getKey(), DocNode.wrap(entry.getValue()))) //
                    .collect(Collectors.toList());
            return ImmutableList.of(mutableList);
        }

        public ImmutableSet<String> instanceIds() {
            return watchInstances.keySet();
        }
    }
}