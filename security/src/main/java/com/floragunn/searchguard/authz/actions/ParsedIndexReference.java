package com.floragunn.searchguard.authz.actions;

import com.floragunn.searchsupport.meta.Meta;

/**
 * Examples:
 *  "my-index"           → ParsedIndexReference("my-index", false)
 *  "my-index::failures" → ParsedIndexReference("my-index", true)
 *  "my-*::failures"     → ParsedIndexReference("my-*", true)
 *
 * @param baseName the base index name without the component separator suffix (e.g., "my-index" or "my-*")
 * @param failureStore {@code true} if this reference points to a failure store (i.e., the original
 *                     index reference had the "::failures" suffix), {@code false} otherwise
 */
public record ParsedIndexReference(String baseName, boolean failureStore) {

    public ParsedIndexReference {
        assert (baseName == null) || (!baseName.contains(Meta.COMPONENT_SEPARATOR)) : "Unexpected component separator";
    }

    public ParsedIndexReference withIndexName(String indexWithoutComponent) {
        return new ParsedIndexReference(indexWithoutComponent, failureStore);
    }

    /**
     * Return name conform with {@link Meta.IndexLikeObject#name()}
     * @return name conform with {@link Meta.IndexLikeObject#name()}
     */
    public String metaName() {
        if(baseName == null) {
            return null;
        }
        return failureStore ? baseName + Meta.FAILURES_SUFFIX : baseName;
    }

}
