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

import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;

public class UnparsedDocumentTest {

    private static final byte[] UTF8 = new byte[] { 0x7b, 0x22, 0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x22, 0x3a, 0x20, 0x22, (byte) 0xe4, (byte) 0xbd,
            (byte) 0xa0, (byte) 0xe5, (byte) 0xa5, (byte) 0xbd, 0x22, 0x7d };

    private static final byte[] UTF16_BE = new byte[] { (byte) 0xfe, (byte) 0xff, 0x00, 0x7b, 0x00, 0x22, 0x00, 0x68, 0x00, 0x65, 0x00, 0x6c, 0x00,
            0x6c, 0x00, 0x6f, 0x00, 0x22, 0x00, 0x3a, 0x00, 0x20, 0x00, 0x22, 0x4f, 0x60, 0x59, 0x7d, 0x00, 0x22, 0x00, 0x7d };

    private static final byte[] UTF16_LE = new byte[] { (byte) 0xff, (byte) 0xfe, 0x7b, 0x00, 0x22, 0x00, 0x68, 0x00, 0x65, 0x00, 0x6c, 0x00, 0x6c,
            0x00, 0x6f, 0x00, 0x22, 0x00, 0x3a, 0x00, 0x20, 0x00, 0x22, 0x00, 0x60, 0x4f, 0x7d, 0x59, 0x22, 0x00, 0x7d, 0x00 };

    private static final byte[] UTF32_BE = new byte[] { 0x00, 0x00, (byte) 0xfe, (byte) 0xff, 0x00, 0x00, 0x00, 0x7b, 0x00, 0x00, 0x00, 0x22, 0x00,
            0x00, 0x00, 0x68, 0x00, 0x00, 0x00, 0x65, 0x00, 0x00, 0x00, 0x6c, 0x00, 0x00, 0x00, 0x6c, 0x00, 0x00, 0x00, 0x6f, 0x00, 0x00, 0x00, 0x22,
            0x00, 0x00, 0x00, 0x3a, 0x00, 0x00, 0x00, 0x20, 0x00, 0x00, 0x00, 0x22, 0x00, 0x00, 0x4f, 0x60, 0x00, 0x00, 0x59, 0x7d, 0x00, 0x00, 0x00,
            0x22, 0x00, 0x00, 0x00, 0x7d };

    @Test
    public void testEncodingConversion() throws DocumentParseException {
        UnparsedDocument<?> unparsedDoc = UnparsedDocument.from(UTF16_BE, Format.JSON);
        Assert.assertEquals("你好", unparsedDoc.parseAsDocNode().get("hello"));
        unparsedDoc = UnparsedDocument.from(UTF16_LE, Format.JSON);
        Assert.assertEquals("你好", unparsedDoc.parseAsDocNode().get("hello"));
        unparsedDoc = UnparsedDocument.from(UTF32_BE, Format.JSON);
        Assert.assertEquals("你好", unparsedDoc.parseAsDocNode().get("hello"));
        unparsedDoc = UnparsedDocument.from(UTF8, Format.JSON);
        Assert.assertEquals("你好", unparsedDoc.parseAsDocNode().get("hello"));
    }

    @Test
    public void testDocTypeConversion() throws DocumentParseException {
        UnparsedDocument<?> unparsedDoc = UnparsedDocument.from("{\"a\": [1,2,3]}", Format.JSON);
        Assert.assertEquals(Arrays.asList(1, 2, 3), UnparsedDocument.from(unparsedDoc.toYamlString(), Format.YAML).parseAsDocNode().get("a"));
    }
}
