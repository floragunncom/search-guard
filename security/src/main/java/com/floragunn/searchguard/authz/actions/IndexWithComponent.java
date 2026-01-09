package com.floragunn.searchguard.authz.actions;

import com.floragunn.searchsupport.meta.Component;

public record IndexWithComponent(String indexName, Component component) {

    public String indexNameWithComponent() {
        return component.indexLikeNameWithComponentSuffix(indexName);
    }

}
