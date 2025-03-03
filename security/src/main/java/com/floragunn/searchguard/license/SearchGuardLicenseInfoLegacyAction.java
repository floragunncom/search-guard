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

package com.floragunn.searchguard.license;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.injection.guice.Inject;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.modules.api.GetComponentStateAction;
import com.floragunn.searchsupport.action.Action;
import com.floragunn.searchsupport.action.RestApi;
import com.floragunn.searchsupport.action.StandardRequests.EmptyRequest;

public class SearchGuardLicenseInfoLegacyAction extends Action<EmptyRequest, SearchGuardLicenseInfoLegacyAction.Response> {
    public static final SearchGuardLicenseInfoLegacyAction INSTANCE = new SearchGuardLicenseInfoLegacyAction();
    public static final String NAME = "cluster:admin:searchguard:get_license_info_legacy";

    public static final RestApi REST_API = new RestApi()//
            .handlesGet("/_searchguard/license").with(SearchGuardLicenseInfoLegacyAction.INSTANCE).name("/_searchguard/license");

    protected SearchGuardLicenseInfoLegacyAction() {
        super(NAME, EmptyRequest::new, Response::new);
    }

    public static class Response extends Action.Response {

        private SearchGuardLicense license;
        private Map<String, Set<String>> licensesRequired;

        public Response(SearchGuardLicense license, Map<String, Set<String>> licensesRequired) {
            this.license = license;
            this.licensesRequired = licensesRequired;
        }

        public Response(UnparsedMessage message) throws ConfigValidationException {
            super(message);
            DocNode docNode = message.requiredDocNode();
            this.license = new SearchGuardLicense(docNode.getAsNode("sg_license"));
            this.licensesRequired = toMultiMap(docNode.getAsNode("licenses_required").toMap());
        }

        @Override
        public Object toBasicObject() {
            Map<String, Object> result = new LinkedHashMap<>();
            @SuppressWarnings("unchecked")
            Map<String, Object> license = (Map<String, Object>) this.license.toBasicObject();

            license.put("license_required", !licensesRequired.isEmpty());
            
            result.put("sg_license", license);
            result.put("licenses_required", licensesRequired);

            return result;
        }

        private Map<String, Set<String>> toMultiMap(Map<String, Object> map) {
            LinkedHashMap<String, Set<String>> result = new LinkedHashMap<String, Set<String>>();

            for (Map.Entry<String, Object> entry : map.entrySet()) {
                if (entry.getValue() instanceof Collection) {
                    result.put(entry.getKey(), ((Collection<?>) entry.getValue()).stream().map((e) -> String.valueOf(e)).collect(Collectors.toSet()));
                } else {
                    result.put(entry.getKey(), ImmutableSet.of(String.valueOf(entry.getValue())));
                }
            }

            return result;
        }

    }

    public static class Handler extends Action.Handler<EmptyRequest, Response> {

        private final LicenseRepository licenseRepository;
        private final NodeClient nodeClient;

        @Inject
        public Handler(HandlerDependencies handlerDependencies, LicenseRepository licenseRepository, NodeClient nodeClient) {
            super(SearchGuardLicenseInfoLegacyAction.INSTANCE, handlerDependencies);
            this.licenseRepository = licenseRepository;
            this.nodeClient = nodeClient;
        }

        @Override
        protected CompletableFuture<Response> doExecute(EmptyRequest request) {
            CompletableFuture<Response> result = new CompletableFuture<>();

            SearchGuardLicense license = licenseRepository.getLicense();

            nodeClient.execute(GetComponentStateAction.INSTANCE, new GetComponentStateAction.Request((String) null, true),
                    new ActionListener<GetComponentStateAction.Response>() {

                        @Override
                        public void onResponse(GetComponentStateAction.Response response) {
                            result.complete(new Response(license, response.getComponentsGroupedByLicense()));
                        }

                        @Override
                        public void onFailure(Exception e) {
                            result.completeExceptionally(e);
                        }

                    });

            return result;
        }

    }

}
