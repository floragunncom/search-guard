/*
 * Based on https://github.com/elastic/elasticsearch/blob/5c8b0662df09a5d7a53fd0d98b1d37b1d831ddbc/server/src/main/java/org/elasticsearch/cluster/metadata/IndexNameExpressionResolver.java
 * from Apache 2 licensed Elasticsearch 7.10.2.
 * 
 * Original license header:
 * 
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * 
 * 
 * Modifications:
 * 
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
 */

package com.floragunn.searchsupport.queries;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.function.Predicate;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.regex.Regex;

import com.floragunn.fluent.collections.ImmutableMap;

public class WildcardExpressionResolver {

    public static Map<String, IndexAbstraction> matches(Metadata metadata, SortedMap<String, IndexAbstraction> indicesLookup, String expression,
            IndicesOptions options, boolean includeDataStreams) {

        if (Regex.isMatchAllPattern(expression)) {
            return filterIndicesLookup(metadata, indicesLookup, expression, null, options, includeDataStreams);
        } else if (expression.indexOf("*") == expression.length() - 1) {
            return suffixWildcard(metadata, indicesLookup, expression, options, includeDataStreams);
        } else {
            return otherWildcard(metadata, indicesLookup, expression, options, includeDataStreams);
        }
    }

    public static List<String> resolveEmptyOrTrivialWildcard(IndicesOptions options, Metadata metadata) {
        ProjectMetadata project = metadata.getProject(Metadata.DEFAULT_PROJECT_ID);
        if (options.expandWildcardsOpen() && options.expandWildcardsClosed() && options.expandWildcardsHidden()) {
            return Arrays.asList(project.getConcreteAllIndices());
        } else if (options.expandWildcardsOpen() && options.expandWildcardsClosed()) {
            return Arrays.asList(project.getConcreteVisibleIndices());
        } else if (options.expandWildcardsOpen() && options.expandWildcardsHidden()) {
            return Arrays.asList(project.getConcreteAllOpenIndices());
        } else if (options.expandWildcardsOpen()) {
            return Arrays.asList(project.getConcreteVisibleOpenIndices());
        } else if (options.expandWildcardsClosed() && options.expandWildcardsHidden()) {
            return Arrays.asList(project.getConcreteAllClosedIndices());
        } else if (options.expandWildcardsClosed()) {
            return Arrays.asList(project.getConcreteVisibleClosedIndices());
        } else {
            return Collections.emptyList();
        }
    }

    public static boolean isEmptyOrTrivialWildcard(List<String> expressions) {
        return expressions.isEmpty()
                || (expressions.size() == 1 && (Metadata.ALL.equals(expressions.get(0)) || Regex.isMatchAllPattern(expressions.get(0))));
    }

    private static Map<String, IndexAbstraction> suffixWildcard(Metadata metadata, SortedMap<String, IndexAbstraction> indicesLookup,
            String expression, IndicesOptions options, boolean includeDataStreams) {
        String fromPrefix = expression.substring(0, expression.length() - 1);
        char[] toPrefixCharArr = fromPrefix.toCharArray();
        toPrefixCharArr[toPrefixCharArr.length - 1]++;
        String toPrefix = new String(toPrefixCharArr);
        SortedMap<String, IndexAbstraction> subMap = indicesLookup.subMap(fromPrefix, toPrefix);
        return filterIndicesLookup(metadata, subMap, expression, null, options, includeDataStreams);
    }

    private static Map<String, IndexAbstraction> otherWildcard(Metadata metadata, SortedMap<String, IndexAbstraction> indicesLookup,
            String expression, IndicesOptions options, boolean includeDataStreams) {
        final String pattern = expression;
        return filterIndicesLookup(metadata, indicesLookup, expression, e -> Regex.simpleMatch(pattern, e.getKey()), options, includeDataStreams);
    }

    private static Map<String, IndexAbstraction> filterIndicesLookup(Metadata metadata, SortedMap<String, IndexAbstraction> indicesLookup,
            String expression, Predicate<? super Map.Entry<String, IndexAbstraction>> filter, IndicesOptions options, boolean includeDataStreams) {
        IndexMetadata.State excludeState = excludeState(options);

        if (!options.ignoreAliases() && includeDataStreams && filter == null && options.expandWildcardsHidden() && excludeState == null) {
            return indicesLookup;
        }

        ImmutableMap.Builder<String, IndexAbstraction> result = new ImmutableMap.Builder<String, IndexAbstraction>();

        for (Map.Entry<String, IndexAbstraction> entry : indicesLookup.entrySet()) {

            IndexAbstraction.Type type = entry.getValue().getType();

            if (options.ignoreAliases() && type == IndexAbstraction.Type.ALIAS) {
                continue;
            }

            if (!includeDataStreams && type == IndexAbstraction.Type.DATA_STREAM) {
                continue;
            }

            if (!options.expandWildcardsHidden() && entry.getValue().isHidden() && !implicitHiddenMatch(entry.getKey(), expression)) {
                continue;
            }

            if (filter != null && !filter.test(entry)) {
                continue;
            }

            if (excludeState != null && type == IndexAbstraction.Type.CONCRETE_INDEX) {
                ProjectMetadata project = metadata.getProject(Metadata.DEFAULT_PROJECT_ID);
                IndexMetadata indexMetadata = project.index(entry.getKey());

                if (indexMetadata != null && excludeState == indexMetadata.getState()) {
                    continue;
                }
            }

            result.put(entry.getKey(), entry.getValue());
        }

        return result.build();
    }

    public static IndexMetadata.State excludeState(IndicesOptions options) {
        final IndexMetadata.State excludeState;
        if (options.expandWildcardsOpen() && options.expandWildcardsClosed()) {
            excludeState = null;
        } else if (options.expandWildcardsOpen() && options.expandWildcardsClosed() == false) {
            excludeState = IndexMetadata.State.CLOSE;
        } else if (options.expandWildcardsClosed() && options.expandWildcardsOpen() == false) {
            excludeState = IndexMetadata.State.OPEN;
        } else {
            excludeState = null;
        }
        return excludeState;
    }
    
    public static Predicate<Boolean> excludeStatePredicate(IndicesOptions options) {
        IndexMetadata.State excludeState = excludeState(options);
        
        if (excludeState == IndexMetadata.State.OPEN) {
            return (b) -> b == true;
        } else if (excludeState == IndexMetadata.State.CLOSE) {
            return (b) -> b == false;
        } else {
            return null;
        }
    }

    private static boolean implicitHiddenMatch(String itemName, String expression) {
        return itemName.startsWith(".") && expression.startsWith(".") && Regex.isSimpleMatchPattern(expression);
    }

}
