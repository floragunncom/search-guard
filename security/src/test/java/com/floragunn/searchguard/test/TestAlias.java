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
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import com.floragunn.searchsupport.Constants;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.elasticsearch.client.internal.Client;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.fluent.collections.ImmutableSet;

import static com.floragunn.searchsupport.Constants.DEFAULT_ACK_TIMEOUT;
import static com.floragunn.searchsupport.Constants.DEFAULT_MASTER_TIMEOUT;

public class TestAlias implements TestIndexLike {

    private final String name;
    private final ImmutableSet<TestIndexLike> indices;
    private Set<String> documentIds;
    private Map<String, Map<String, ?>> documents;
    private TestIndexLike writeIndex;

    public TestAlias(String name, TestIndexLike... indices) {
        this.name = name;
        this.indices = ImmutableSet.ofArray(indices);
    }

    public TestAlias writeIndex(TestIndexLike writeIndex) {
        this.writeIndex = writeIndex;
        return this;
    }

    @Override
    public DocNode getFieldsMappings() {
        return indices.stream() //
                .map(TestIndexLike::getFieldsMappings) //
                .findFirst() //
                .orElseThrow();
    }

    @Override
    public String toString() {
        return "Test alias name '" + name + "'";
    }

    public void create(Client client) {
        client.admin().indices().aliases(new IndicesAliasesRequest(DEFAULT_MASTER_TIMEOUT, DEFAULT_ACK_TIMEOUT).addAliasAction(AliasActions.add().indices(getIndexNamesAsArray()).alias(name)))
                .actionGet();

        if (writeIndex != null) {
            client.admin().indices()
                    .aliases(new IndicesAliasesRequest(DEFAULT_MASTER_TIMEOUT, DEFAULT_ACK_TIMEOUT).addAliasAction(AliasActions.add().index(writeIndex.getName()).alias(name).writeIndex(true)))
                    .actionGet();
        }
    }

    public void create(GenericRestClient client) throws Exception {
        GenericRestClient.HttpResponse response = client.postJson("_aliases",
                DocNode.of("actions", DocNode.array(DocNode.of("add.indices", getIndexNamesAsArray(), "add.alias", name))));

        if (response.getStatusCode() != 200) {
            throw new RuntimeException("Error while creating alias " + name + "\n" + response);
        }
    }

    public String getName() {
        return name;
    }

    public ImmutableSet<TestIndexLike> getIndices() {
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
            for (TestIndexLike testIndex : this.indices) {
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
            for (TestIndexLike testIndex : this.indices) {
                result.putAll(testIndex.getDocuments());
            }

            result = Collections.unmodifiableMap(result);
            this.documents = result;
        }

        return result;
    }

    @Override
    public Optional<TestIndexLike> failureStore() {
        boolean hasFailureStore = indices.stream().anyMatch(indexLike -> indexLike.failureStore().isPresent());
        if (hasFailureStore) {
            return Optional.of(new TestAlias(name + "::failures",
                    indices.stream() //
                            .map(TestIndexLike::failureStore) //
                            .filter(Optional::isPresent) //
                            .map(Optional::get) //
                            .toArray(TestIndexLike[]::new)));
        } else {
            return Optional.empty();
        }
    }

    @Override
    public TestAlias dataOnly() {
        return new TestAlias(name, indices.stream()
                .map(TestIndexLike::dataOnly)
                .toArray(TestIndexLike[]::new));
    }
}