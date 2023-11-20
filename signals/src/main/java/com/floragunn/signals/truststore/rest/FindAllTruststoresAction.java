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
import org.elasticsearch.common.inject.Inject;

import com.floragunn.searchguard.SearchGuardVersion;
import com.floragunn.searchguard.support.PrivilegedConfigClient;
import com.floragunn.searchsupport.action.Action;
import com.floragunn.searchsupport.action.RestApi;
import com.floragunn.searchsupport.action.StandardRequests.EmptyRequest;
import com.floragunn.searchsupport.action.StandardResponse;
import com.floragunn.signals.Signals;
import com.floragunn.signals.truststore.service.TruststoreCrudService;
import com.floragunn.signals.truststore.service.persistence.TruststoreRepository;


public class FindAllTruststoresAction extends Action<EmptyRequest, StandardResponse> {

    private static final Logger log = LogManager.getLogger(FindAllTruststoresAction.class);

    public final static String NAME = "cluster:admin:searchguard:signals:truststores/findall";
    public final static FindAllTruststoresAction INSTANCE = new FindAllTruststoresAction();

    public static final RestApi REST_API = new RestApi()//
        .responseHeaders(SearchGuardVersion.header())//
        .handlesGet("/_signals/truststores")//
        .with(INSTANCE)//
        .name("GET /_signals/truststores");

    private FindAllTruststoresAction() {
        super(NAME, EmptyRequest::new, StandardResponse::new);
    }

    public static class FindAllTruststoresHandler extends Handler<EmptyRequest, StandardResponse> {

        private final TruststoreCrudService truststoreCrudService;

        @Inject
        public FindAllTruststoresHandler(HandlerDependencies handlerDependencies, NodeClient client, Signals signals) {
            super(INSTANCE, handlerDependencies);
            PrivilegedConfigClient privilegedConfigClient = PrivilegedConfigClient.adapt(client);
            TruststoreRepository truststoreRepository = new TruststoreRepository(signals.getSignalsSettings(), privilegedConfigClient);
            this.truststoreCrudService = new TruststoreCrudService(truststoreRepository);
        }

        @Override
        protected CompletableFuture<StandardResponse> doExecute(EmptyRequest request) {
            return supplyAsync(() -> {
                try {
                    return truststoreCrudService.findAll();
                } catch (Exception e) {
                    log.warn("Cannot load trust store data", e);
                    return new StandardResponse(500).error("Cannot load trust store data. " + e.getMessage());
                }
            });
        }
    }
}
