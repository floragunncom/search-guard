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

import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

public class ValidatingDocumentNodeTest {
    @Test
    public void basicTest() {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vNode = new ValidatingDocNode(ImmutableMap.of("a", "A", "b", 2), validationErrors);

        Assert.assertEquals("A", vNode.get("a").asString());
        Assert.assertEquals("2", vNode.get("b").asString());
        Assert.assertNull(vNode.get("c").asString());
        Assert.assertEquals("X", vNode.get("c").withDefault("X").asString());
        Assert.assertNull(vNode.get("c").required().asString());

        Assert.assertEquals("c", validationErrors.getOnlyValidationError().getAttribute());
        Assert.assertEquals("Required attribute is missing", validationErrors.getOnlyValidationError().getMessage());

        validationErrors = new ValidationErrors();
        vNode = new ValidatingDocNode(ImmutableMap.of("a", "A", "b", 2), validationErrors);

        // Even with default value, accessing a required attribute yields a validation error
        Assert.assertEquals("X", vNode.get("c").required().withDefault("X").asString());
        Assert.assertEquals("c", validationErrors.getOnlyValidationError().getAttribute());
        Assert.assertEquals("Required attribute is missing", validationErrors.getOnlyValidationError().getMessage());
    }

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

    @Test
    public void bigDecimalTest() {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vNode = new ValidatingDocNode(ImmutableMap.of("a", "1.234", "b", "x"), validationErrors);

        Assert.assertEquals("1.234", vNode.get("a").asBigDecimal().toString());
        Assert.assertNull(vNode.get("b").asBigDecimal());
        Assert.assertTrue(validationErrors.toDebugString(), validationErrors.toString().contains("invalid value; expected: number"));
    }

    @Test
    public void listAttributeTest() {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vNode = new ValidatingDocNode(ImmutableMap.of("a", "1", "b", Arrays.asList("1", "2")), validationErrors);

        Assert.assertEquals(Arrays.asList("1"), vNode.get("a").asListOfStrings());
        Assert.assertEquals(Arrays.asList("1", "2"), vNode.get("b").asListOfStrings());
        Assert.assertNull(vNode.get("c").asListOfStrings());
    }

    @Test
    public void enumTest() {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vNode = new ValidatingDocNode(ImmutableMap.of("a", "e1", "b", "E2", "c", "ex"), validationErrors);

        Assert.assertEquals(TestEnum.E1, vNode.get("a").asEnum(TestEnum.class));
        Assert.assertEquals(TestEnum.E2, vNode.get("b").asEnum(TestEnum.class));
        Assert.assertNull(vNode.get("c").asEnum(TestEnum.class));

        Assert.assertEquals("c", validationErrors.getOnlyValidationError().getAttribute());
        Assert.assertEquals("E1|E2", validationErrors.getOnlyValidationError().getExpectedAsString());

        Assert.assertNull(vNode.get("d").asEnum(TestEnum.class));
    }

    public static enum TestEnum {
        E1, E2;
    }
}
