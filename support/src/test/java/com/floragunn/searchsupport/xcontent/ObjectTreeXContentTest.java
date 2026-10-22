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

package com.floragunn.searchsupport.xcontent;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class ObjectTreeXContentTest {

    private static final ToXContentObject STRINGS_FROM_READERS = (builder, params) -> {
        builder.startObject();
        builder.field("bounded");
        builder.generator().writeString(new StringReader("hello world"), 5);
        builder.field("unbounded");
        builder.generator().writeString(new StringReader("all of it"), -1);
        return builder.endObject();
    };

    @Test
    public void writeStringFromReader_objectTree() {
        assertThat(ObjectTreeXContent.toObjectTree(STRINGS_FROM_READERS), equalTo(Map.of("bounded", "hello", "unbounded", "all of it")));
    }

    @Test
    public void writeStringFromReader_attributeValue() {
        assertThat(AttributeValueFromXContent.get(STRINGS_FROM_READERS, "bounded"), equalTo("hello"));
        assertThat(AttributeValueFromXContent.get(STRINGS_FROM_READERS, "unbounded"), equalTo("all of it"));
    }

    @Test
    public void closeAllowIllFormed_objectTree() throws IOException {
        try (XContentBuilder builder = XContentBuilder.builder(new ObjectTreeXContent(HashMap::new))) {
            builder.startObject().field("a", 1);
            builder.generator().closeAllowIllFormed();
            assertThat(((ObjectTreeXContent.Generator) builder.generator()).getTopLevelObject(), equalTo(Map.of("a", 1)));
        }
    }

    @Test
    public void closeAllowIllFormed_attributeValue() throws IOException {
        AttributeValueFromXContent xContent = new AttributeValueFromXContent("a");
        try (XContentBuilder builder = XContentBuilder.builder(xContent)) {
            builder.startObject().field("a", 1);
            builder.generator().closeAllowIllFormed();
            assertThat(xContent.getAttributeValue(), equalTo(1));
        }
    }
}
