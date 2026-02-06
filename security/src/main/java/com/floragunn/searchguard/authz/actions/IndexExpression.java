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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.metadata.Metadata;

import com.floragunn.searchsupport.meta.Meta;

/**
 * Represents a parsed index expression, splitting a raw index string into a base name and an optional
 * component selector ({@code ::data} or {@code ::failures}).
 *
 * <p>Elasticsearch index expressions can include a component selector suffix separated by
 * {@link Meta#COMPONENT_SEPARATOR} ({@code "::"}). This class parses such expressions and exposes
 * the base index name and whether the expression targets the failure store.</p>
 *
 * <h3>Parsing rules ({@link #of(String)}):</h3>
 * <ul>
 *   <li>{@code null} input is normalized to a base name of {@link Metadata#ALL} ({@code "_all"})
 *       with {@code failureStore = false}.</li>
 *   <li>An expression without {@code "::"} is treated as a plain index name
 *       with {@code failureStore = false}.</li>
 *   <li>An expression ending with {@code "::data"} is parsed as the base name before the separator
 *       with {@code failureStore = false}.</li>
 *   <li>An expression ending with {@code "::failures"} is parsed as the base name before the separator
 *       with {@code failureStore = true}.</li>
 *   <li>An expression containing {@code "::"} followed by an <em>unknown</em> component selector
 *       is kept as-is (the full expression including the separator becomes the base name)
 *       with {@code failureStore = false}. An error is logged in this case.</li>
 *   <li>When multiple {@code "::"} sequences appear, only the <em>last</em> occurrence is used as
 *       the component separator.</li>
 * </ul>
 *
 * <h3>Examples:</h3>
 * <pre>
 *  IndexExpression.of(null)                → baseName="_all",     failureStore=false
 *  IndexExpression.of("my-index")          → baseName="my-index", failureStore=false
 *  IndexExpression.of("my-index::data")    → baseName="my-index", failureStore=false
 *  IndexExpression.of("my-index::failures")→ baseName="my-index", failureStore=true
 *  IndexExpression.of("my-*::failures")    → baseName="my-*",     failureStore=true
 *  IndexExpression.of("my-index::unknown") → baseName="my-index::unknown", failureStore=false  (error logged)
 * </pre>
 */
class IndexExpression {

    private static final Logger log = LogManager.getLogger(IndexExpression.class);

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

    private IndexExpression(String baseName, boolean failureStore) {
        if (baseName == null) {
            baseName = Metadata.ALL;
        }
        this.baseName = baseName;
        this.failureStore = failureStore;
    }

    public static IndexExpression of(String expression) {
        if (expression == null) {
            return new IndexExpression(null, false);
        }
        int lastIndexOfComponentSeparator = expression.lastIndexOf(Meta.COMPONENT_SEPARATOR);
        if (lastIndexOfComponentSeparator == -1) {
            return new IndexExpression(expression, false);
        } else {
            String indexName = expression.substring(0, lastIndexOfComponentSeparator);
            String componentSuffix = expression.substring(lastIndexOfComponentSeparator);
            if (Meta.FAILURES_SUFFIX.equals(componentSuffix)) {
                return new IndexExpression(indexName, true);
            } else if (Meta.DATA_SUFFIX.equals(componentSuffix)) {
                return new IndexExpression(indexName, false);
            } else {
                log.error("Unknown component selector '{}' in index expression: '{}'.", componentSuffix, expression);
                // In this case baseName will contain Meta.COMPONENT_SEPARATOR with component selector
                return new IndexExpression(expression, false);
            }
        }
    }

    public String baseName() {
        return baseName;
    }

    public boolean failureStore() {
        return failureStore;
    }

    public IndexExpression withIndexName(String indexWithoutComponent) {
        return new IndexExpression(indexWithoutComponent, failureStore);
    }

    /**
     * Return name conform with {@link Meta.IndexLikeObject#name()}
     * @return name conform with {@link Meta.IndexLikeObject#name()}
     */
    public String metaName() {
        return failureStore ? baseName + Meta.FAILURES_SUFFIX : baseName;
    }

    public boolean isExclusion() {
        return baseName.startsWith("-");
    }

    public IndexExpression dropExclusion() {
        if(isExclusion()) {
            return new IndexExpression(baseName.substring(1), failureStore);
        }
        return this;
    }

    public boolean containsStarWildcard() {
        return baseName.contains("*");
    }

    public boolean  containsWildcard() {
        return ActionRequestIntrospector.containsWildcard(baseName);
    }

    public IndexExpression mapBaseName(Function<String, String> mapper) {
        return new IndexExpression(mapper.apply(baseName), failureStore);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IndexExpression that = (IndexExpression) o;
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
        int firstIndex = baseName.indexOf(REMOTE_CLUSTER_INDEX_SEPARATOR);
        int lastIndex = baseName.lastIndexOf(REMOTE_CLUSTER_INDEX_SEPARATOR);

        // If both are same and not -1, there's exactly one colon
        return (firstIndex != -1) && (firstIndex == lastIndex);
    }
}
