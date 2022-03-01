package com.floragunn.searchguard.user;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;

import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;

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
