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

package com.floragunn.signals.proxy.rest;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.DocumentParseException;
import com.floragunn.codova.documents.UnparsedDocument;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.searchguard.SearchGuardVersion;
import com.floragunn.searchguard.support.PrivilegedConfigClient;
import com.floragunn.searchsupport.action.Action;
import com.floragunn.searchsupport.action.RestApi;
import com.floragunn.searchsupport.action.StandardRequests;
import com.floragunn.searchsupport.action.StandardResponse;
import com.floragunn.signals.Signals;
import com.floragunn.signals.proxy.service.NoSuchProxyException;
import com.floragunn.signals.proxy.service.ProxyCrudService;
import com.floragunn.signals.proxy.service.persistence.ProxyRepository;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.injection.guice.Inject;

import java.util.concurrent.CompletableFuture;

public class ProxyApi {

    public static final RestApi REST_API = new RestApi()//
            .responseHeaders(SearchGuardVersion.header())//
            .handlesPut("/_signals/proxies/{id}")//
            .with(CreateOrReplaceProxyAction.INSTANCE, (params, body) -> new CreateOrReplaceProxyAction.CreateOrReplaceProxyRequest(params.get("id"), body))//
            .handlesDelete("/_signals/proxies/{id}")//
            .with(DeleteProxyAction.INSTANCE, (params, body) -> new StandardRequests.IdRequest(params.get("id")))//
            .handlesGet("/_signals/proxies")//
            .with(FindAllProxiesAction.INSTANCE)//
            .handlesGet("/_signals/proxies/{id}")//
            .with(FindOneProxyAction.INSTANCE, (params, body) -> new StandardRequests.IdRequest(params.get("id")))//
            .name("/_signals/proxies/");

    public static class CreateOrReplaceProxyAction extends
            Action<CreateOrReplaceProxyAction.CreateOrReplaceProxyRequest, StandardResponse> {

        private static final Logger log = LogManager.getLogger(CreateOrReplaceProxyAction.class);

        public static final String NAME = "cluster:admin:searchguard:signals:proxies/createorreplace";
        public static final CreateOrReplaceProxyAction INSTANCE = new CreateOrReplaceProxyAction();

        public CreateOrReplaceProxyAction() {
            super(NAME, CreateOrReplaceProxyAction.CreateOrReplaceProxyRequest::new, StandardResponse::new);
        }

        public static class CreateOrUpdateProxyHandler extends Handler<CreateOrReplaceProxyAction.CreateOrReplaceProxyRequest, StandardResponse> {

            private final ProxyCrudService proxyCrudService;

            private final NodeClient client;

            @Inject
            public CreateOrUpdateProxyHandler(HandlerDependencies handlerDependencies, NodeClient client, Signals signals) {
                super(INSTANCE, handlerDependencies);
                PrivilegedConfigClient privilegedConfigClient = PrivilegedConfigClient.adapt(client);
                ProxyRepository proxyRepository = new ProxyRepository(signals.getSignalsSettings(), privilegedConfigClient);
                this.client = client;
                this.proxyCrudService = new ProxyCrudService(proxyRepository);
            }

            @Override
            protected CompletableFuture<StandardResponse> doExecute(CreateOrReplaceProxyRequest request) {
                return supplyAsync(() -> {
                    try {
                        StandardResponse response = proxyCrudService.createOrReplace(request);
                        log.debug("Proxy with id '{}' stored in index.", request.getId());
                        TransportProxyUpdatedAction.ProxyUpdatedActionType.send(client, request.getId(), "create-or-update").actionGet();
                        log.debug("Notification related to proxy '{}' update send.", request.getId());
                        return response;
                    } catch (ConfigValidationException e) {
                        log.debug("Cannot create or replace proxy", e);
                        return new StandardResponse(400).error(e);
                    } catch (Exception e) {
                        log.error("Cannot create or replace proxy", e);
                        return new StandardResponse(500).error("Cannot create or replace proxy. " + e.getMessage());
                    }
                });
            }
        }

        public static class CreateOrReplaceProxyRequest extends Action.Request {

            public static final String FIELD_ID = "id";
            public static final String FIELD_NAME = "name";
            public static final String FIELD_URI = "uri";
            private final String id;
            private final String name;
            private final String uri;

            public CreateOrReplaceProxyRequest(UnparsedMessage message) throws ConfigValidationException {
                DocNode docNode = message.requiredDocNode();
                this.id = docNode.getAsString(FIELD_ID);
                this.name = docNode.getAsString(FIELD_NAME);
                this.uri = docNode.getAsString(FIELD_URI);
            }

            public CreateOrReplaceProxyRequest(String id, UnparsedDocument<?> message) throws DocumentParseException {
                DocNode docNode = message.parseAsDocNode();
                this.id = id;
                this.name = docNode.getAsString(FIELD_NAME);
                this.uri = docNode.getAsString(FIELD_URI);
            }

            @Override
            public Object toBasicObject() {
                return ImmutableMap.of(FIELD_ID, id, FIELD_NAME, name, FIELD_URI, uri);
            }

            public String getId() {
                return id;
            }

            public String getName() {
                return name;
            }

            public String getUri() {
                return uri;
            }
        }
    }

    public static class DeleteProxyAction extends Action<StandardRequests.IdRequest, StandardResponse> {

        private static final Logger log = LogManager.getLogger(DeleteProxyAction.class);

        public final static String NAME = "cluster:admin:searchguard:signals:proxies/delete";
        public final static DeleteProxyAction INSTANCE = new DeleteProxyAction();

        private DeleteProxyAction() {
            super(NAME, StandardRequests.IdRequest::new, StandardResponse::new);
        }

        public static class DeleteProxyHandler extends Handler<StandardRequests.IdRequest, StandardResponse> {

            private final ProxyCrudService proxyCrudService;
            private final NodeClient client;

            @Inject
            public DeleteProxyHandler(HandlerDependencies handlerDependencies, NodeClient client, Signals signals) {
                super(INSTANCE, handlerDependencies);
                PrivilegedConfigClient privilegedConfigClient = PrivilegedConfigClient.adapt(client);
                ProxyRepository proxyRepository = new ProxyRepository(signals.getSignalsSettings(), privilegedConfigClient);
                this.proxyCrudService = new ProxyCrudService(proxyRepository);
                this.client = client;
            }

            @Override
            protected CompletableFuture<StandardResponse> doExecute(StandardRequests.IdRequest request) {
                return supplyAsync(() -> {
                    try {
                        StandardResponse response = proxyCrudService.delete(request.getId());
                        log.debug("Proxy '{}' deleted", request.getId());
                        TransportProxyUpdatedAction.ProxyUpdatedActionType.send(client, request.getId(), "delete").actionGet();
                        log.debug("Notification related to proxy '{}' deletion send.", request.getId());
                        return response;
                    } catch (Exception e) {
                        log.warn("Cannot delete proxy '{}' data", request.getId(), e);
                        return new StandardResponse(500).error("Cannot delete proxy data. " + e.getMessage());
                    }
                });
            }
        }
    }

    public static class FindAllProxiesAction extends Action<StandardRequests.EmptyRequest, StandardResponse> {

        private static final Logger log = LogManager.getLogger(FindAllProxiesAction.class);

        public final static String NAME = "cluster:admin:searchguard:signals:proxies/findall";
        public final static FindAllProxiesAction INSTANCE = new FindAllProxiesAction();

        private FindAllProxiesAction() {
            super(NAME, StandardRequests.EmptyRequest::new, StandardResponse::new);
        }

        public static class FindAllProxiesHandler extends Handler<StandardRequests.EmptyRequest, StandardResponse> {

            private final ProxyCrudService proxyCrudService;

            @Inject
            public FindAllProxiesHandler(HandlerDependencies handlerDependencies, NodeClient client, Signals signals) {
                super(INSTANCE, handlerDependencies);
                PrivilegedConfigClient privilegedConfigClient = PrivilegedConfigClient.adapt(client);
                ProxyRepository proxyRepository = new ProxyRepository(signals.getSignalsSettings(), privilegedConfigClient);
                this.proxyCrudService = new ProxyCrudService(proxyRepository);
            }

            @Override
            protected CompletableFuture<StandardResponse> doExecute(StandardRequests.EmptyRequest request) {
                return supplyAsync(() -> {
                    try {
                        return proxyCrudService.findAll();
                    } catch (Exception e) {
                        log.warn("Cannot load proxies data", e);
                        return new StandardResponse(500).error("Cannot load proxies data. " + e.getMessage());
                    }
                });
            }
        }
    }

    public static class FindOneProxyAction extends Action<StandardRequests.IdRequest, StandardResponse> {

        private static final Logger log = LogManager.getLogger(FindOneProxyAction.class);

        public final static String NAME = "cluster:admin:searchguard:signals:proxies/findone";
        public final static FindOneProxyAction INSTANCE = new FindOneProxyAction();

        private FindOneProxyAction() {
            super(NAME, StandardRequests.IdRequest::new, StandardResponse::new);
        }

        public static class FindOneProxyHandler extends Handler<StandardRequests.IdRequest, StandardResponse> {

            private final ProxyCrudService proxyCrudService;

            @Inject
            public FindOneProxyHandler(HandlerDependencies handlerDependencies, NodeClient client, Signals signals) {
                super(INSTANCE, handlerDependencies);
                PrivilegedConfigClient privilegedConfigClient = PrivilegedConfigClient.adapt(client);
                ProxyRepository proxyRepository = new ProxyRepository(signals.getSignalsSettings(), privilegedConfigClient);
                this.proxyCrudService = new ProxyCrudService(proxyRepository);
            }

            @Override
            protected CompletableFuture<StandardResponse> doExecute(StandardRequests.IdRequest request) {
                return supplyAsync(() -> {
                    try {
                        return proxyCrudService.findOne(request.getId());
                    } catch (NoSuchProxyException e) {
                        log.debug("Proxy '{}' not found", request.getId(), e);
                        return new StandardResponse(404).error(e.getMessage());
                    } catch (Exception e) {
                        log.warn("Cannot load proxy '{}' data", request.getId(), e);
                        return new StandardResponse(500).error("Cannot load proxy data. " + e.getMessage());
                    }
                });
            }
        }
    }
}
