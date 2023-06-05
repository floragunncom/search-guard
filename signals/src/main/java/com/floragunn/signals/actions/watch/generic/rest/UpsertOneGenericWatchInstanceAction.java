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
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.searchguard.SearchGuardVersion;
import com.floragunn.searchsupport.action.RestApi;
import com.floragunn.searchsupport.action.StandardResponse;
import com.floragunn.searchsupport.action.Action;
import com.floragunn.signals.Signals;
import com.floragunn.signals.actions.watch.generic.service.GenericWatchService;
import com.floragunn.signals.actions.watch.generic.service.GenericWatchServiceFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.threadpool.ThreadPool;


public class UpsertOneGenericWatchInstanceAction extends Action<UpsertOneGenericWatchInstanceAction.UpsertOneGenericWatchInstanceRequest, StandardResponse> {

    private static final Logger log = LogManager.getLogger(UpsertOneGenericWatchInstanceAction.class);

    public final static String NAME = "cluster:admin:searchguard:tenant:signals:watch/instances/upsert_one";
    public static final UpsertOneGenericWatchInstanceAction INSTANCE = new UpsertOneGenericWatchInstanceAction();

    public static final RestApi REST_API = new RestApi()
        .responseHeaders(SearchGuardVersion.header())//
        .handlesPut("/_signals/watch/{tenant}/{id}/instances/{instance_id}")//
        .with(INSTANCE, (params, body) -> new UpsertOneGenericWatchInstanceRequest(params.get("tenant"), params.get("id"),
            params.get("instance_id"), body))//
        .name("PUT /_signals/watch/{tenant}/{id}/instances/{instance_id}");

    public UpsertOneGenericWatchInstanceAction() {
        super(NAME, UpsertOneGenericWatchInstanceRequest::new, StandardResponse::new);
    }

    public static class UpsertOneGenericWatchInstanceHandler extends StandardSyncOperationHandler<UpsertOneGenericWatchInstanceRequest> {

        private final GenericWatchService genericWatchService;

        @Inject
        public UpsertOneGenericWatchInstanceHandler(NodeClient client, Signals signals, HandlerDependencies handlerDependencies,
            ThreadPool threadPool) {
            super(INSTANCE, handlerDependencies, threadPool);
            this.genericWatchService = new GenericWatchServiceFactory(signals, client).create();
        }

        @Override
        protected StandardResponse synchronousExecute(UpsertOneGenericWatchInstanceRequest request) {
            try {
                return genericWatchService.upsert(request);
            } catch (ConfigValidationException e) {
                log.error("Cannot create generic watch instance because validation errors occurred.", e);
                return new StandardResponse(400) //
                    .message("Cannot create generic watch instance because validation errors occurred.") //
                    .error(e);
            }
        }
    }

    public static class UpsertOneGenericWatchInstanceRequest extends Request {
        public static final String FIELD_PARAMETERS = "parameters";
        private final WatchInstanceIdRepresentation id;
        private final ImmutableMap<String, Object> parameters;

        public UpsertOneGenericWatchInstanceRequest(UnparsedMessage message) throws ConfigValidationException {
            DocNode docNode = message.requiredDocNode();
            this.id = new WatchInstanceIdRepresentation(docNode);
            this.parameters = docNode.getAsNode(FIELD_PARAMETERS).toMap();
        }

        public UpsertOneGenericWatchInstanceRequest(String tenantId, String watchId, String instanceId, UnparsedDocument<?> message)
            throws ConfigValidationException {
            if(message == null) {
                ValidationError validationError = new ValidationError("body",
                    "Request body is required and should contains watch template parameters");
                throw new ConfigValidationException(validationError);
            }
            DocNode docNode = message.parseAsDocNode();
            this.parameters = docNode.toMap();
            this.id = new WatchInstanceIdRepresentation(tenantId, watchId, instanceId);
        }

        UpsertOneGenericWatchInstanceRequest(String tenantId, String watchId, String instanceId, DocNode message) {
            this.parameters = message.toMap();
            this.id = new WatchInstanceIdRepresentation(tenantId, watchId, instanceId);
        }

        @Override
        public Object toBasicObject() {
            return this.id.toBasicObject().with(FIELD_PARAMETERS, parameters);
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

        public ImmutableMap<String, Object> getParameters() {
            return parameters;
        }
    }
}