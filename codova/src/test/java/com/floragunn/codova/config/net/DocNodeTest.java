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

package com.floragunn.codova.config.net;

import org.junit.Assert;
import org.junit.Test;

import com.floragunn.codova.documents.DocNode;
import com.google.common.collect.ImmutableMap;

public class DocNodeTest {

    @Test
    public void ofTest() {
        DocNode docNode = DocNode.of("a", 1, "b.a", 10, "b.b", 11, "c", ImmutableMap.of("a", 20, "b", 21, "c", 22));

        Assert.assertEquals(1, docNode.get("a"));
        Assert.assertEquals(10, docNode.get("b.a"));
        Assert.assertEquals(ImmutableMap.of("a", 10, "b", 11), docNode.get("b"));
        Assert.assertEquals(20, docNode.get("c.a"));
        Assert.assertEquals(ImmutableMap.of("a", 20, "b", 21, "c", 22), docNode.get("c"));
    }
}
