/*
 * Copyright 2021-2022 floragunn GmbH
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

import java.util.stream.Collectors;

import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.elasticsearch.client.Client;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.fluent.collections.ImmutableSet;

public class TestAlias {

    private final String name;
    private final ImmutableSet<TestIndex> indices;

    public TestAlias(String name, TestIndex... indices) {
        this.name = name;
        this.indices = ImmutableSet.ofArray(indices);
    }

    public void create(Client client) {
        client.admin().indices().aliases(new IndicesAliasesRequest().addAliasAction(AliasActions.add().indices(getIndexNamesAsArray()).alias(name)))
                .actionGet();
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

    public ImmutableSet<TestIndex> getIndices() {
        return indices;
    }

    public String[] getIndexNamesAsArray() {
        return indices.stream().map(i -> i.getName()).collect(Collectors.toSet()).toArray(new String[0]);
    }
}