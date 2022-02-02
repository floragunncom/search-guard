/*
 * Copyright 2022 floragunn GmbH
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

package com.floragunn.codova.documents.patch;

import org.junit.Assert;
import org.junit.Test;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.DocumentParseException;
import com.floragunn.codova.documents.Format;
import com.floragunn.codova.validation.ConfigValidationException;

public class SimplePathPatchTest {
    @Test
    public void basicTest() {
        DocNode targetDocument = DocNode.of("a", "ao", "b", DocNode.of("b_x", "bo1", "b_z", "bo3"), "c", "co");
        SimplePathPatch simplePathPatch = new SimplePathPatch(new SimplePathPatch.Operation("b.b_x", 44));
        DocNode resultDocument = simplePathPatch.apply(targetDocument);

        Assert.assertEquals(DocNode.of("a", "ao", "b", DocNode.of("b_x", 44, "b_z", "bo3"), "c", "co").toJsonString(), resultDocument.toJsonString());
    }

    @Test
    public void parseTest() throws DocumentParseException, ConfigValidationException {
        DocNode targetDocument = DocNode.of("a", "ao", "b", DocNode.of("b_x", "bo1", "b_z", "bo3"), "c", "co");
        SimplePathPatch jsonPathPatch = new SimplePathPatch(new SimplePathPatch.Operation("b.b_x", 44));
        SimplePathPatch jsonPathPatch2 = new SimplePathPatch(DocNode.parse(Format.JSON).from(jsonPathPatch.toJsonString()));

        Assert.assertEquals(jsonPathPatch, jsonPathPatch2);

        DocNode resultDocument = jsonPathPatch.apply(targetDocument);

        Assert.assertEquals(DocNode.of("a", "ao", "b", DocNode.of("b_x", 44, "b_z", "bo3"), "c", "co").toJsonString(), resultDocument.toJsonString());
    }
}
