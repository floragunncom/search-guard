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

import com.floragunn.codova.documents.Document;
import com.floragunn.fluent.collections.ImmutableMap;

public class TestComponentTemplate implements Document<TestComponentTemplate> {
    public static TestComponentTemplate DATA_STREAM_MINIMAL = new TestComponentTemplate("test_component_template_data_stream_minimal",
            new TestMapping(new TestMapping.Property("@timestamp", "date", "date_optional_time||epoch_millis")));

    private String name;
    private TestMapping mapping;

    public TestComponentTemplate(String name, TestMapping mapping) {
        this.name = name;
        this.mapping = mapping;
    }

    public String getName() {
        return name;
    }

    public TestMapping getMapping() {
        return mapping;
    }

    public void create(GenericRestClient client) throws Exception {
        GenericRestClient.HttpResponse response = client.putJson("/_component_template/" + name, this.toJsonString());

        if (response.getStatusCode() != 200) {
            throw new RuntimeException("Error while creating component template " + name + "\n" + response);
        }
    }

    @Override
    public Object toBasicObject() {
        return ImmutableMap.of("template", ImmutableMap.of("mappings", this.mapping.toBasicObject()));
    }

}
