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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.injection.guice.Inject;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.DocumentParseException;
import com.floragunn.codova.documents.UnparsedDocument;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.searchguard.SearchGuardVersion;
import com.floragunn.searchguard.support.PrivilegedConfigClient;
import com.floragunn.searchsupport.action.Action;
import com.floragunn.searchsupport.action.RestApi;
import com.floragunn.searchsupport.action.StandardResponse;
import com.floragunn.signals.Signals;
import com.floragunn.signals.truststore.service.TruststoreCrudService;
import com.floragunn.signals.truststore.service.persistence.TruststoreRepository;

public class CreateOrReplaceTruststoreAction extends
    Action<CreateOrReplaceTruststoreAction.CreateOrReplaceTruststoreRequest, StandardResponse> {

    private static final Logger log = LogManager.getLogger(CreateOrReplaceTruststoreAction.class);

    public static final String NAME = "cluster:admin:searchguard:signals:truststores/createorreplace";
    public static final CreateOrReplaceTruststoreAction INSTANCE = new CreateOrReplaceTruststoreAction();

    public static final RestApi REST_API = new RestApi()//
        .responseHeaders(SearchGuardVersion.header())//
        .handlesPut("/_signals/truststores/{id}")//
        .with(INSTANCE, (params, body) -> new CreateOrReplaceTruststoreRequest(params.get("id"), body))//
        .name("PUT /_signals/truststores/{id}");

    public CreateOrReplaceTruststoreAction() {
        super(NAME, CreateOrReplaceTruststoreRequest::new, StandardResponse::new);
    }

    public static class UploadTruststoreHandler extends Handler<CreateOrReplaceTruststoreRequest, StandardResponse> {

        private final TruststoreCrudService truststoreCrudService;

        private final NodeClient client;

        @Inject
        public UploadTruststoreHandler(HandlerDependencies handlerDependencies, NodeClient client, Signals signals) {
            super(INSTANCE, handlerDependencies);
            PrivilegedConfigClient privilegedConfigClient = PrivilegedConfigClient.adapt(client);
            TruststoreRepository truststoreRepository = new TruststoreRepository(signals.getSignalsSettings(), privilegedConfigClient);
            this.client = client;
            this.truststoreCrudService = new TruststoreCrudService(truststoreRepository);
        }

        @Override
        protected CompletableFuture<StandardResponse> doExecute(CreateOrReplaceTruststoreRequest request) {
            return supplyAsync(() -> {
                try {
                    StandardResponse response = truststoreCrudService.createOrReplace(request);
                    log.debug("Truststore with id '{}' stored in index.", request.getId());
                    TransportTruststoreUpdatedAction.TruststoreUpdatedActionType.send(client, request.getId(), "create-or-update").actionGet();
                    log.debug("Notification related to truststore '{}' update send.", request.getId());
                    return response;
                } catch (ConfigValidationException e) {
                    log.error("Cannot create or replace truststore", e);
                    return new StandardResponse(400).error("Cannot create or replace truststore. " + e.getMessage());
                }
            });
        }
    }

    public static class CreateOrReplaceTruststoreRequest extends Action.Request {
        private final static String ENCAPSULATION_BOUNDARY_END = "-----END CERTIFICATE-----";
        public static final String FIELD_NAME = "name";
        public static final String FIELD_PEM = "pem";
        public static final String FIELD_ID = "id";
        private final String id;
        private final String name;
        private final String pem;

        public CreateOrReplaceTruststoreRequest(UnparsedMessage message) throws ConfigValidationException {
            DocNode docNode = message.requiredDocNode();
            this.id = docNode.getAsString(FIELD_ID);
            this.name = docNode.getAsString(FIELD_NAME);
            this.pem = docNode.getAsString(FIELD_PEM);
        }

        public CreateOrReplaceTruststoreRequest(String id, UnparsedDocument<?> message) throws DocumentParseException {
            DocNode docNode = message.parseAsDocNode();
            this.id = id;
            this.name = docNode.getAsString(FIELD_NAME);
            this.pem = docNode.getAsString(FIELD_PEM);
        }

        @Override
        public Object toBasicObject() {
            return ImmutableMap.of(FIELD_ID, id, FIELD_NAME, name, FIELD_PEM, pem);
        }

        public List<String> getCertificates() {
            return splitCertificatesByEncapsulationBoundaries();
        }

        private List<String> splitCertificatesByEncapsulationBoundaries() {
            List<String> splitCertificates = new ArrayList<>();
            String certificates = pem;
            int splitIndex;
            while((splitIndex = certificates.indexOf(ENCAPSULATION_BOUNDARY_END)) != -1) {
                splitIndex += ENCAPSULATION_BOUNDARY_END.length();
                String currentCertificate = certificates.substring(0, splitIndex);
                certificates = certificates.substring(splitIndex);
                splitCertificates.add(currentCertificate.trim());
            }
            return splitCertificates;
        }

        public String getId() {
            return id;
        }

        public String getName() {
            return name;
        }

        public String getPem() {
            return pem;
        }
    }
}
