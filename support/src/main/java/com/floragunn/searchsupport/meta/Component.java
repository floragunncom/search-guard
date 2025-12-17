package com.floragunn.searchsupport.meta;

public enum Component {

    NONE(""), FAILURES("failures");

    public static final String COMPONENT_SEPARATOR = "::";

    private final String componentSuffix;

    Component(String componentSuffix) {
        this.componentSuffix = componentSuffix;
    }

    public boolean includesData() {
        return this == NONE;
    }

    public boolean includesFailures() {
        return this == FAILURES;
    }

    public String indexLikeNameWithComponentSuffix(String indexLikeName) {
        if (this.componentSuffix.isEmpty()) {
            return indexLikeName;
        } else {
            if (indexLikeName.endsWith(COMPONENT_SEPARATOR.concat(componentSuffix))) {
                return indexLikeName;
            }
            return indexLikeName.concat(COMPONENT_SEPARATOR).concat(componentSuffix);
        }
    }
}
