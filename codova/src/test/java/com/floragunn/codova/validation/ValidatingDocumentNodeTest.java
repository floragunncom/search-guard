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

package com.floragunn.codova.validation;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

public class ValidatingDocumentNodeTest {
    @Test
    public void baseUrlTest() throws Exception {

        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vNode = new ValidatingDocNode(ImmutableMap.of("x", "http://www.example.com"), validationErrors);

        Assert.assertEquals("http://www.example.com/", vNode.get("x").asBaseURL().toString());

        vNode = new ValidatingDocNode(ImmutableMap.of("x", "http://www.example.com/"), validationErrors);
        Assert.assertEquals("http://www.example.com/", vNode.get("x").asBaseURL().toString());

        vNode = new ValidatingDocNode(ImmutableMap.of("x", "http://www.example.com/a"), validationErrors);
        Assert.assertEquals("http://www.example.com/a/", vNode.get("x").asBaseURL().toString());

        vNode = new ValidatingDocNode(ImmutableMap.of("x", "http://www.example.com/a/"), validationErrors);
        Assert.assertEquals("http://www.example.com/a/", vNode.get("x").asBaseURL().toString());

    }

}
