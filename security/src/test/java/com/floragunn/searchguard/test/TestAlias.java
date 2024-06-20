/*
 * Copyright 2021-2024 floragunn GmbH
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

package com.floragunn.searchguard.test;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.elasticsearch.client.Client;

import com.floragunn.fluent.collections.ImmutableSet;

public class TestAlias implements TestIndexLike {

    private final String name;
    private final ImmutableSet<TestIndex> indices;
    private Set<String> documentIds;
    private Map<String, Map<String, ?>> documents;
    private TestIndex writeIndex;

    public TestAlias(String name, TestIndex... indices) {
        this.name = name;
        this.indices = ImmutableSet.ofArray(indices);
    }

    public TestAlias writeIndex(TestIndex writeIndex) {
        this.writeIndex = writeIndex;
        return this;
    }

    public void create(Client client) {
        client.admin().indices().aliases(new IndicesAliasesRequest().addAliasAction(AliasActions.add().indices(getIndexNamesAsArray()).alias(name)))
                .actionGet();

        if (writeIndex != null) {
            client.admin().indices()
                    .aliases(new IndicesAliasesRequest().addAliasAction(AliasActions.add().index(writeIndex.getName()).alias(name).writeIndex(true)))
                    .actionGet();
        }
    }

    public String getName() {
        return name;
    }

    public ImmutableSet<TestIndex> getIndices() {
        return indices;
    }

    public String[] getIndexNamesAsArray() {
        return indices.stream().map(i -> i.getName()).collect(Collectors.toSet()).toArray(new String[0]);
    }

    @Override
    public Set<String> getDocumentIds() {
        Set<String> result = this.documentIds;

        if (result == null) {
            result = new HashSet<>();
            for (TestIndex testIndex : this.indices) {
                result.addAll(testIndex.getDocumentIds());
            }

            result = Collections.unmodifiableSet(result);
            this.documentIds = result;
        }

        return result;
    }

    @Override
    public Map<String, Map<String, ?>> getDocuments() {
        Map<String, Map<String, ?>> result = this.documents;

        if (result == null) {
            result = new HashMap<>();
            for (TestIndex testIndex : this.indices) {
                result.putAll(testIndex.getDocuments());
            }

            result = Collections.unmodifiableMap(result);
            this.documents = result;
        }

        return result;
    }
}