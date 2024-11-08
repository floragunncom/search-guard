/*
 * Copyright 2024 floragunn GmbH
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

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Document;
import com.floragunn.fluent.collections.ImmutableList;

public class TestIndexTemplate implements Document<TestIndexTemplate> {
    public static final TestIndexTemplate DATA_STREAM_MINIMAL = new TestIndexTemplate("test_index_template_data_stream_minimal", "ds_*").dataStream()
            .composedOf(TestComponentTemplate.DATA_STREAM_MINIMAL);

    private String name;
    private ImmutableList<String> indexPatterns;
    private DocNode dataStream;
    private ImmutableList<TestComponentTemplate> composedOf = ImmutableList.empty();
    private int priority = 0;

    public TestIndexTemplate(String name, String... indexPatterns) {
        this.name = name;
        this.indexPatterns = ImmutableList.ofArray(indexPatterns);
    }

    public TestIndexTemplate dataStream() {
        this.dataStream = DocNode.EMPTY;
        return this;
    }

    public TestIndexTemplate dataStream(String k, Object v) {
        this.dataStream = DocNode.of(k, v);
        return this;
    }

    public TestIndexTemplate composedOf(TestComponentTemplate... composedOf) {
        this.composedOf = ImmutableList.ofArray(composedOf);
        return this;
    }

    public TestIndexTemplate priority(int priority) {
        this.priority = priority;
        return this;
    }

    @Override
    public Object toBasicObject() {
        return DocNode.of("index_patterns", indexPatterns, "priority", priority, "data_stream", dataStream, "composed_of",
                composedOf.map(TestComponentTemplate::getName));
    }

    public String getName() {
        return name;
    }

    public ImmutableList<TestComponentTemplate> getComposedOf() {
        return composedOf;
    }

    public void create(GenericRestClient client) throws Exception {
        GenericRestClient.HttpResponse response = client.putJson("/_index_template/" + name, this.toJsonString());

        if (response.getStatusCode() != 200) {
            throw new RuntimeException("Error while creating component template " + name + "\n" + response);
        }
    }
}
