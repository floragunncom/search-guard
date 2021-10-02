/*
 * Copyright 2021 floragunn GmbH
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

package com.floragunn.codova.documents;

import org.junit.Assert;
import org.junit.Test;

import com.floragunn.codova.documents.DocType.UnknownDocTypeException;
import com.google.common.base.Charsets;

public class ContentTypeTest {
    @Test
    public void testParse() throws UnknownDocTypeException {
        Assert.assertEquals(DocType.JSON, ContentType.parseHeader("application/json").getDocType());
        Assert.assertEquals(DocType.JSON, ContentType.parseHeader("text/x-json; charset=utf-8").getDocType());
        Assert.assertEquals(Charsets.UTF_8, ContentType.parseHeader("text/x-json; charset=utf-8").getCharset());
        Assert.assertEquals(DocType.YAML, ContentType.parseHeader("application/yaml; whatever=foo; charset=utf-16; x=y").getDocType());
        Assert.assertEquals(Charsets.UTF_16, ContentType.parseHeader("application/yaml; whatever=foo; charset=utf-16; x=y").getCharset());
    }

    @Test
    public void testUnknownContentType() {
        try {
            ContentType contentType = ContentType.parseHeader("image/jpeg");
            Assert.fail(contentType.toString());
        } catch (UnknownDocTypeException e) {

        }
    }
}
