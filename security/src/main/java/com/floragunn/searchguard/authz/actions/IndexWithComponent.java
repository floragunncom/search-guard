package com.floragunn.searchguard.authz.actions;

import com.floragunn.searchsupport.meta.Meta;

public record IndexWithComponent(String indexNameWithoutComponent, boolean failureStore) {

    public IndexWithComponent {
        assert (indexNameWithoutComponent == null) || (!indexNameWithoutComponent.contains(Meta.COMPONENT_SEPARATOR)) : "Unexpected component separator";
    }

    public IndexWithComponent withIndexName(String indexWithoutComponent) {
        return new IndexWithComponent(indexWithoutComponent, failureStore);
    }

    public String indexNamePossiblyWithComponent() {
        if(indexNameWithoutComponent == null) {
            return null;
        }
        return failureStore ? indexNameWithoutComponent + Meta.FAILURES_SUFFIX : indexNameWithoutComponent;
    }

}
