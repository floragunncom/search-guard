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

import com.floragunn.codova.validation.ConfigValidationException;

public class DocNodeTest {
    @Test
    public void parseEmptyDocument() throws Exception {
        try {
            DocNode docNode = DocNode.parse(DocType.JSON).from("");
            Assert.fail(docNode.toString());
        } catch (ConfigValidationException e) {
            Assert.assertEquals("The document is empty", e.getMessage());
        }
    }

    @Test
    public void parseWhitespaceDocument() throws Exception {
        try {
            DocNode docNode = DocNode.parse(DocType.JSON).from("   ");
            Assert.fail(docNode.toString());
        } catch (ConfigValidationException e) {
            Assert.assertEquals("The document is empty", e.getMessage());
        }
    }
}
