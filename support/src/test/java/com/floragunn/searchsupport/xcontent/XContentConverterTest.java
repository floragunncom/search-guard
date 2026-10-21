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

package com.floragunn.searchsupport.xcontent;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.DocumentParseException;
import com.floragunn.codova.documents.Format;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xcontent.ToXContent;
import org.junit.Test;

import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.containsValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class XContentConverterTest {

    private static final Logger log = LogManager.getLogger(XContentConverterTest.class);
    public static final String VALUE_ONE = "one";
    public static final String VALUE_TWO = "two";

    @Test 
    public void shouldSupportsNulls() {
        assertThat(XContentConverter.canConvert(null), equalTo(false));
        assertThat(XContentConverter.convertOrNull(null), nullValue());
    }

    @Test 
    public void shouldCastChunkedToXContentToToXContent() throws DocumentParseException {
        TestChunkedToXContentObjectImpl testChunkedToXContentObjectImpl = new TestChunkedToXContentObjectImpl(VALUE_ONE, VALUE_TWO);

        ToXContent toXContent = XContentConverter.convertOrNull(testChunkedToXContentObjectImpl);

        assertThat(XContentConverter.canConvert(testChunkedToXContentObjectImpl), equalTo(true));
        String serialized = Strings.toString(toXContent, ToXContent.EMPTY_PARAMS);
        log.warn("Serialized object '{}'", serialized);
        DocNode docNode = DocNode.parse(Format.JSON).from(serialized);
        assertThat(docNode, containsValue("$.first", VALUE_ONE));
        assertThat(docNode, containsValue("$.second", VALUE_TWO));
    }

    @Test 
    public void shouldCastToXContent() throws DocumentParseException {
        TestToXContentObjectImpl testToXContentObjectImpl = new TestToXContentObjectImpl(VALUE_ONE);

        ToXContent toXContent = XContentConverter.convertOrNull(testToXContentObjectImpl);

        assertThat(XContentConverter.canConvert(testToXContentObjectImpl), equalTo(true));
        String serialized = Strings.toString(toXContent, ToXContent.EMPTY_PARAMS);
        log.warn("Serialized object '{}'", serialized);
        DocNode docNode = DocNode.parse(Format.JSON).from(serialized);
        assertThat(docNode, containsValue("$.msg", VALUE_ONE));
    }

    @Test 
    public void shouldNotCastToXContent() {
        Object object = "I am not ToXContent neither ChunkedToXContent";

        assertThat(XContentConverter.convertOrNull(object), nullValue());
        assertThat(XContentConverter.canConvert(object), equalTo(false));
    }
}