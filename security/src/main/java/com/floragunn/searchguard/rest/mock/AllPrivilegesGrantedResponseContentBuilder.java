/*
 * Copyright 2026 floragunn GmbH
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

package com.floragunn.searchguard.rest.mock;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.fluent.collections.ImmutableSet;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

class AllPrivilegesGrantedResponseContentBuilder {

    static Map<String, Object> buildAllPrivilegesGrantedResponseContent(String user, DocNode requestBody) {
        Map<String, Object> result = new HashMap<>(requestBody.size());
        result.put("username", user);
        result.put("has_all_requested", true);
        result.put("cluster", markAllClusterPrivilegesAsGranted(requestBody));
        result.put("index", markAllIndexPrivilegesAsGranted(requestBody));
        result.put("application", markAllApplicationPrivilegesAsGranted(requestBody));
        return result;
    }

    static Map<String, Boolean> markAllClusterPrivilegesAsGranted(DocNode requestBody) {
        if (requestBody.hasNonNull("cluster")) {
            Set<String> clusterPermsFromRequest = ImmutableSet.of(requestBody.getAsListOfStrings("cluster"));
            return markPrivilegesAsGranted(clusterPermsFromRequest);
        } else {
            return Collections.emptyMap();
        }
    }

    static Map<String, Map<String, Boolean>> markAllIndexPrivilegesAsGranted(DocNode requestBody) {
        if (requestBody.hasNonNull("index")) {
            List<DocNode> indicesPrivilegesFromRequest = requestBody.getAsListOfNodes("index");
            Map<String, Map<String, Boolean>> grantedIndicesPrivileges = new HashMap<>();

            indicesPrivilegesFromRequest.forEach(indicesAndPrivileges -> {
                Set<String> indexNames = ImmutableSet.of(indicesAndPrivileges.getAsListOfStrings("names"));
                Set<String> privileges = ImmutableSet.of(indicesAndPrivileges.getAsListOfStrings("privileges"));
                Map<String, Map<String, Boolean>> grantedIndexPrivileges = markResourcesPrivilegesAsGranted(indexNames, privileges);

                grantedIndexPrivileges.forEach((index, grantedPrivileges) -> grantedIndicesPrivileges
                        .computeIfAbsent(index, i -> new HashMap<>(grantedPrivileges.size()))
                        .putAll(grantedPrivileges)
                );
            });
            return grantedIndicesPrivileges;
        } else {
            return Collections.emptyMap();
        }
    }

    static Map<String, Map<String, Map<String, Boolean>>> markAllApplicationPrivilegesAsGranted(DocNode requestBody) {
        if (requestBody.hasNonNull("application")) {
            List<DocNode> applicationsResourcesPrivilegesFromRequest = requestBody.getAsListOfNodes("application");
            Map<String, Map<String, Map<String, Boolean>>> grantedApplicationsResourcesPrivileges = new HashMap<>();

            applicationsResourcesPrivilegesFromRequest.forEach(applicationResourcesAndPrivileges -> {
                String application = applicationResourcesAndPrivileges.getAsString("application");
                Set<String> resources = ImmutableSet.of(applicationResourcesAndPrivileges.getAsListOfStrings("resources"));
                Set<String> privileges = ImmutableSet.of(applicationResourcesAndPrivileges.getAsListOfStrings("privileges"));
                Map<String, Map<String, Boolean>> grantedResourcePrivileges = markResourcesPrivilegesAsGranted(resources, privileges);

                Map<String, Map<String, Boolean>> grantedApplicationResourcesPrivileges = grantedApplicationsResourcesPrivileges
                        .computeIfAbsent(application, app -> new HashMap<>(grantedResourcePrivileges.size()));

                grantedResourcePrivileges.forEach((resource, markedPrivileges) -> grantedApplicationResourcesPrivileges
                        .computeIfAbsent(resource, r -> new HashMap<>(markedPrivileges.size()))
                        .putAll(markedPrivileges)
                );
            });
            return grantedApplicationsResourcesPrivileges;
        } else {
            return Collections.emptyMap();
        }
    }

    private static Map<String, Map<String, Boolean>> markResourcesPrivilegesAsGranted(Set<String> resources, Set<String> privileges) {
        Map<String, Boolean> grantedPrivileges = markPrivilegesAsGranted(privileges);

        Map<String, Map<String, Boolean>> resourcesPrivilegesMarkedAsGranted = new HashMap<>(resources.size());
        resources.forEach(resource -> resourcesPrivilegesMarkedAsGranted.put(resource, grantedPrivileges));
        return resourcesPrivilegesMarkedAsGranted;
    }

    private static Map<String, Boolean> markPrivilegesAsGranted(Set<String> privileges) {
        Map<String, Boolean> grantedPrivileges = new HashMap<>(privileges.size());
        privileges.forEach(privilege -> grantedPrivileges.put(privilege, true));
        return grantedPrivileges;
    }

}
