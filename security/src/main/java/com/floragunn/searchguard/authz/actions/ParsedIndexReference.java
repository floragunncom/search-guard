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

package com.floragunn.searchguard.authz.actions;

import java.util.Objects;
import java.util.function.Function;

import com.floragunn.searchsupport.meta.Meta;

/**
 * Examples:
 *  "my-index"           → ParsedIndexReference("my-index", false)
 *  "my-index::data"     → ParsedIndexReference("my-index", false)
 *  "my-index::failures" → ParsedIndexReference("my-index", true)
 *  "my-*::failures"     → ParsedIndexReference("my-*", true)
 */
class ParsedIndexReference {

    public static final String REMOTE_CLUSTER_INDEX_SEPARATOR = ":";

    /**
     * The base index name without the component suffix (e.g., "my-index" or "my-*")
     */
    private final String baseName;

    /**
     * {@code true} if this reference points to a failure store (i.e., the original
     * index reference had the "::failures" suffix), {@code false} otherwise
     */
    private final boolean failureStore;

    private ParsedIndexReference(String baseName, boolean failureStore) {
        assert (baseName == null) || (!baseName.contains(Meta.COMPONENT_SEPARATOR)) : "Unexpected component separator";
        this.baseName = baseName;
        this.failureStore = failureStore;
    }

    public static ParsedIndexReference of(String indexExpression) {
        if (indexExpression == null) {
            return new ParsedIndexReference(null, false);
        }
        int lastIndexOfComponentSeparator = indexExpression.lastIndexOf(Meta.COMPONENT_SEPARATOR);
        if (lastIndexOfComponentSeparator == -1) {
            return new ParsedIndexReference(indexExpression, false);
        } else {
            String indexName = indexExpression.substring(0, lastIndexOfComponentSeparator);
            String componentSuffix = indexExpression.substring(lastIndexOfComponentSeparator);
            if (Meta.FAILURES_SUFFIX.equals(componentSuffix)) {
                return new ParsedIndexReference(indexName, true);
            } else if (Meta.DATA_SUFFIX.equals(componentSuffix)) {
                return new ParsedIndexReference(indexName, false);
            } else {
                throw new IllegalArgumentException(
                        "Unknown component selector '" + componentSuffix + "' in index expression: " + indexExpression);
            }
        }
    }

    public String baseName() {
        return baseName;
    }

    public boolean failureStore() {
        return failureStore;
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

    public boolean isExclusion() {
        return (baseName != null) && baseName.startsWith("-");
    }

    public ParsedIndexReference dropExclusion() {
        if(isExclusion()) {
            return new ParsedIndexReference(baseName.substring(1), failureStore);
        }
        return this;
    }

    public boolean containsStarWildcard() {
        return (baseName != null) && baseName.contains("*");
    }

    public ParsedIndexReference mapBaseName(Function<String, String> mapper) {
        return new ParsedIndexReference(mapper.apply(baseName), failureStore);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ParsedIndexReference that = (ParsedIndexReference) o;
        return failureStore == that.failureStore && Objects.equals(baseName, that.baseName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(baseName, failureStore);
    }

    @Override
    public String toString() {
        return "ParsedIndexReference[baseName=" + baseName + ", failureStore=" + failureStore + "]";
    }

    public boolean isRemoteIndex() {
        if (baseName == null) {
            return false;
        }
        return baseName.contains(REMOTE_CLUSTER_INDEX_SEPARATOR);
    }
}
