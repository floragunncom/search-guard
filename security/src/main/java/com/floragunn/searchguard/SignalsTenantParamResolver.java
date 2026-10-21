/*
 * Copyright 2022-2026 floragunn GmbH
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

package com.floragunn.searchguard;

import org.elasticsearch.rest.RestRequest;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

public class SignalsTenantParamResolver {

    //this is quick and dirty hack to get the tenant from the uri path

    private SignalsTenantParamResolver() {

    }

    public static String getRequestedTenant(RestRequest request) {
        //TODO ES8 RestHandler refactoring: check precedence
        //not sure if precedence is correct here
        final Optional<String> tenantFromUri = getSignalsTenantFrom(request);

        if (tenantFromUri.isPresent()) {
            String tenantParamValue = URLDecoder.decode(tenantFromUri.get(), StandardCharsets.UTF_8);

            if ("_main".equals(tenantParamValue)) {
                return null;
            } else {
                return tenantParamValue;
            }
        } else {
            return request.header("sgtenant") != null ? request.header("sgtenant") : request.header("sg_tenant");
        }
    }


    public static Optional<String> getSignalsTenantFrom(RestRequest request) {
        if(request.uri().startsWith("/_signals/watch/") || request.uri().startsWith("/_signals/tenant/")) {
            return Optional.of(request.uri().split("/")[3]);
        }

        String path = request.path();
        if (path.startsWith("/_signals/account/")) {
            String[] pathParts = path.split("/");
            boolean tenantAccountCrud = pathParts.length == 6;
            boolean tenantAccountSearch = pathParts.length == 5 && "_search".equals(pathParts[4]);
            if (tenantAccountCrud || tenantAccountSearch) {
                return Optional.of(pathParts[3]);
            }
        }

        return Optional.empty();
    }
}
