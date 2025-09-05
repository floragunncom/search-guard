package com.floragunn.signals.actions.summary;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public record WatchActionNames(String watchIdWithTenantPrefix, List<String> actionNames, Boolean active) {
    public WatchActionNames {
        if (watchIdWithTenantPrefix == null || watchIdWithTenantPrefix.isBlank()) {
            throw new IllegalArgumentException("watchIdWithTenantPrefix must not be null or empty");
        }
        if (actionNames == null) {
            throw new IllegalArgumentException("actionNames must not be null");
        }
    }

    public Set<String> actionNamesAsSet() {
        return new HashSet<>(actionNames);
    }
}
