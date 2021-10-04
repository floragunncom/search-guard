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
import java.util.Collections;

import org.junit.Assert;
import org.junit.Test;

import com.floragunn.codova.validation.ConfigValidationException;
import com.google.common.collect.ImmutableMap;

public class DocNodeTest {
    @Test
    public void basicAccess() throws Exception {
        String doc = "" //
                + "a: 42\n"//
                + "b: x\n"//
                + "c:\n"//
                + "  - 1\n"//
                + "  - 2\n"//
                + "  - 3\n"//
                + "d: [6,7,8]\n"//
                + "e:\n"//
                + "  ea: 43\n"//
                + "  eb: y\n"//
                + "  ec:\n"//
                + "    - 10\n"//
                + "    - 20\n"//
                + "    - 30\n"//
                + "  ed: [60,70,80]\n"//
                + "  ee:\n"//
                + "    eea: 44\n"//
                + "    eeb: z\n"//
                + "    eec:\n"//
                + "      - 100\n"//
                + "      - 200\n"//
                + "      - 300\n"//
                + "e.ff.fff: 99";

        DocNode docNode = DocNode.parse(DocType.YAML).from(doc);

        Assert.assertNull(docNode.get("x"));
        Assert.assertTrue(docNode.getAsNode("x").isNull());
        Assert.assertEquals(0, docNode.getAsNode("x").size());
        Assert.assertEquals(Collections.emptyList(), docNode.getAsNode("x").toList());

        Assert.assertEquals(42, docNode.get("a"));
        Assert.assertEquals(42, docNode.getAsNode("a").toBasicObject());
        Assert.assertFalse(docNode.getAsNode("a").isNull());
        Assert.assertNull(docNode.getAsNode("a").get("b"));
        Assert.assertTrue(docNode.getAsNode("a").getAsNode("b").isNull());

        Assert.assertEquals("x", docNode.get("b"));
        Assert.assertEquals(Arrays.asList("x"), docNode.getAsNode("b").toList());

        Assert.assertEquals(Arrays.asList(1, 2, 3), docNode.get("c"));
        Assert.assertEquals(Arrays.asList(1, 2, 3), docNode.getAsNode("c").toList());
        Assert.assertEquals(Arrays.asList("1", "2", "3"), docNode.getAsNode("c").toListOfStrings());
        Assert.assertEquals(3, docNode.getAsNode("c").size());

        Assert.assertEquals(Arrays.asList(6, 7, 8), docNode.get("d"));
        Assert.assertEquals(3, docNode.getAsNode("d").size());

        Assert.assertEquals(ImmutableMap.of("eea", 44, "eeb", "z", "eec", Arrays.asList(100, 200, 300)), docNode.get("e", "ee"));
        Assert.assertEquals(ImmutableMap.of("eea", 44, "eeb", "z", "eec", Arrays.asList(100, 200, 300)),
                docNode.getAsNode("e", "ee").toBasicObject());

        Assert.assertEquals(44, docNode.get("e", "ee", "eea"));

        Assert.assertEquals(Arrays.asList(100, 200, 300), docNode.getAsNode("e", "ee", "eec").toList());

    }

    @Test
    public void unnormalDataAccess() throws Exception {
        String doc = "" //
                + "e:\n"//
                + "  ea: 43\n"//
                + "  eb: y\n"//
                + "  ee:\n"//
                + "    eea: 44\n"//
                + "    eeb: z\n"//
                + "e.ee.eec: 100\n"//
                + "e.ff.fff: 99";

        DocNode docNode = DocNode.parse(DocType.YAML).from(doc);

        Assert.assertEquals(44, docNode.get("e", "ee", "eea"));
        Assert.assertEquals(99, docNode.get("e", "ff", "fff"));
        Assert.assertEquals(ImmutableMap.of("eea", 44, "eeb", "z", "eec", 100), docNode.get("e", "ee"));
    }

    @Test
    public void nullTest() throws DocParseException {
        String doc = "a: null";
        DocNode docNode = DocNode.parse(DocType.YAML).from(doc);

        Assert.assertNull(docNode.get("a"));
        Assert.assertTrue(docNode.getAsNode("a").isNull());
        Assert.assertEquals(0, docNode.getAsListOfNodes("a").size());
        Assert.assertEquals(Collections.emptyList(), docNode.getAsListOfNodes("a"));
        Assert.assertEquals(Collections.emptyList(), docNode.getAsNode("a").toList());

        Assert.assertNull(docNode.get("b"));
        Assert.assertTrue(docNode.getAsNode("b").isNull());
        Assert.assertEquals(0, docNode.getAsListOfNodes("b").size());
        Assert.assertEquals(Collections.emptyList(), docNode.getAsListOfNodes("b"));
        Assert.assertEquals(Collections.emptyList(), docNode.getAsNode("b").toList());

    }

    @Test
    public void without() throws DocParseException {
        String doc = "" //
                + "a: 42\n"//
                + "b: x\n"//
                + "c:\n"//
                + "  - 1\n"//
                + "  - 2\n"//
                + "  - 3\n";

        DocNode docNode = DocNode.parse(DocType.YAML).from(doc);

        Assert.assertEquals(ImmutableMap.of("a", 42, "c", Arrays.asList(1, 2, 3)), docNode.without("b").toMap());
    }

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
