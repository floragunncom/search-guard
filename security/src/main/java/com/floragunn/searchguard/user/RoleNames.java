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
package com.floragunn.searchguard.user;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

public class RoleNames implements ToXContentObject {
    private final Set<String> backendRoles;
    private final Set<String> searchGuardRoles;

    public RoleNames(Set<String> backendRoles, Set<String> searchGuardRoles) {
        this.backendRoles = Collections.unmodifiableSet(backendRoles);
        this.searchGuardRoles = Collections.unmodifiableSet(searchGuardRoles);
    }

    public Set<String> getBackendRoles() {
        return backendRoles;
    }

    public Set<String> getSearchGuardRoles() {
        return searchGuardRoles;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        if (!backendRoles.isEmpty()) {
            builder.field("be", backendRoles);
        }

        if (!searchGuardRoles.isEmpty()) {
            builder.field("sg", searchGuardRoles);
        }

        builder.endObject();
        return builder;
    }

}
