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

package com.floragunn.codova.documents.patch;

import org.junit.Assert;
import org.junit.Test;

import com.floragunn.codova.documents.DocNode;

public class MergePatchTests {

    @Test
    public void basicTest() {
        DocNode targetDocument = DocNode.of("a", "ao", "b", DocNode.of("b_x", "bo1", "b_z", "bo3"), "c", "co");
        MergePatch mergePatch = new MergePatch(DocNode.of("a", "aa", "b", DocNode.of("b_x", "bb1", "b_y", "bb2")));
        DocNode resultDocument = mergePatch.apply(targetDocument);

        Assert.assertEquals(DocNode.of("a", "aa", "b", DocNode.of("b_x", "bb1", "b_z", "bo3", "b_y", "bb2"), "c", "co"), resultDocument);
    }

    @Test
    public void deletePropertyTest() {
        DocNode targetDocument = DocNode.of("a", "ao", "b", DocNode.of("b_x", "bo1", "b_z", "bo3"), "c", "co");
        MergePatch mergePatch = new MergePatch(DocNode.of("a", null, "b", DocNode.of("b_x", "bb1", "b_y", null)));
        DocNode resultDocument = mergePatch.apply(targetDocument);

        Assert.assertEquals(DocNode.of("b", DocNode.of("b_x", "bb1", "b_z", "bo3"), "c", "co"), resultDocument);
    }
}
