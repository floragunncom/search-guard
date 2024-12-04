/*
 * Copyright 2020-2023 floragunn GmbH
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
package com.floragunn.signals.truststore.rest;

import java.util.concurrent.CompletableFuture;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.injection.guice.Inject;

import com.floragunn.searchguard.SearchGuardVersion;
import com.floragunn.searchguard.support.PrivilegedConfigClient;
import com.floragunn.searchsupport.action.Action;
import com.floragunn.searchsupport.action.RestApi;
import com.floragunn.searchsupport.action.StandardRequests.IdRequest;
import com.floragunn.searchsupport.action.StandardResponse;
import com.floragunn.signals.Signals;
import com.floragunn.signals.truststore.service.NoSuchTruststoreException;
import com.floragunn.signals.truststore.service.TruststoreCrudService;
import com.floragunn.signals.truststore.service.persistence.TruststoreRepository;

public class FindOneTruststoreAction extends Action<IdRequest, StandardResponse> {

    private static final Logger log = LogManager.getLogger(FindOneTruststoreAction.class);

    public final static String NAME = "cluster:admin:searchguard:signals:truststores/findone";
    public final static FindOneTruststoreAction INSTANCE = new FindOneTruststoreAction();

    public static final RestApi REST_API = new RestApi()//
        .responseHeaders(SearchGuardVersion.header())//
        .handlesGet("/_signals/truststores/{id}")//
        .with(INSTANCE, (params, body) -> new IdRequest(params.get("id")))//
        .name("GET /_signals/truststores/{id}");
    private FindOneTruststoreAction() {
        super(NAME, IdRequest::new, StandardResponse::new);
    }

    public static class FindOneTruststoreHandler extends Handler<IdRequest, StandardResponse> {

        private final TruststoreCrudService truststoreCrudService;

        @Inject
        public FindOneTruststoreHandler(HandlerDependencies handlerDependencies, NodeClient client, Signals signals) {
            super(INSTANCE, handlerDependencies);
            PrivilegedConfigClient privilegedConfigClient = PrivilegedConfigClient.adapt(client);
            TruststoreRepository truststoreRepository = new TruststoreRepository(signals.getSignalsSettings(), privilegedConfigClient);
            this.truststoreCrudService = new TruststoreCrudService(truststoreRepository);
        }

        @Override
        protected CompletableFuture<StandardResponse> doExecute(IdRequest request) {
            return supplyAsync(() -> {
                try {
                    return truststoreCrudService.findOne(request);
                } catch (NoSuchTruststoreException e) {
                    log.debug("Trust store '{}' not found", request.getId(), e);
                    return new StandardResponse(404).error(e.getMessage());
                } catch (Exception e) {
                    log.warn("Cannot load trust store '{}' data", request.getId(), e);
                    return new StandardResponse(500).error("Cannot parse load store data. " + e.getMessage());
                }
            });
        }
    }
}
