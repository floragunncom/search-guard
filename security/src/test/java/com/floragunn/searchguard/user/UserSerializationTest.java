/*
 * Copyright 2026 floragunn GmbH
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

package com.floragunn.searchguard.user;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.support.Base64Helper;

/**
 * User objects are Java-serialized when they are passed between nodes. Attribute values may originate from non-serializable collection
 * implementations (fluent collections, DocNode). These tests verify that such users can still be serialized.
 */
public class UserSerializationTest {

    @Test
    public void structuredAttributesFromNonSerializableCollections() {
        User user = new User("test_user", AuthDomainInfo.TLS_CERT);
        user.addStructuredAttribute("fluent_map", ImmutableMap.of("a", ImmutableList.of("x", "y"), "b", ImmutableSet.of("z")));
        user.addStructuredAttribute("doc_node", DocNode.of("nested", DocNode.of("k", "v")));
        user.addStructuredAttribute("plain", "value");

        User deserialized = (User) Base64Helper.deserializeObject(Base64Helper.serializeObject(user));

        Assert.assertEquals("test_user", deserialized.getName());
        Map<String, Object> expectedFluentMap = new LinkedHashMap<>();
        expectedFluentMap.put("a", Arrays.asList("x", "y"));
        expectedFluentMap.put("b", Collections.singleton("z"));
        Assert.assertEquals(expectedFluentMap, deserialized.getStructuredAttributes().get("fluent_map"));
        Assert.assertEquals(Collections.singletonMap("nested", Collections.singletonMap("k", "v")),
                deserialized.getStructuredAttributes().get("doc_node"));
        Assert.assertEquals("value", deserialized.getStructuredAttributes().get("plain"));
    }

    @Test
    public void fullConstructorWithNonSerializableCollections() {
        Map<String, String> oldAttributes = new HashMap<>();
        oldAttributes.put("old", "attr");

        User user = new User("test_user", null, AuthDomainInfo.TLS_CERT, null, ImmutableSet.of("backend_role"), ImmutableSet.of("sg_role"), null,
                "tenant", ImmutableMap.of("attr", ImmutableMap.of("x", 1)), oldAttributes, true);

        User deserialized = (User) Base64Helper.deserializeObject(Base64Helper.serializeObject(user));

        Assert.assertEquals(Collections.singleton("backend_role"), deserialized.getRoles());
        Assert.assertEquals(Collections.singleton("sg_role"), deserialized.getSearchGuardRoles());
        Assert.assertEquals("tenant", deserialized.getRequestedTenant());
        Assert.assertEquals(Collections.singletonMap("x", 1), deserialized.getStructuredAttributes().get("attr"));

        // The copies must stay mutable
        user.addRoles(Arrays.asList("another_role"));
        user.addStructuredAttribute("more", "stuff");
        Assert.assertTrue(user.getRoles().contains("another_role"));
    }

    @Test
    public void builderWithNonSerializableCollections() {
        User user = User.forUser("test_user").attribute("fluent", ImmutableMap.of("k", ImmutableList.of(1, 2))).build();

        User deserialized = (User) Base64Helper.deserializeObject(Base64Helper.serializeObject(user));

        Assert.assertEquals(Collections.singletonMap("k", Arrays.asList(1, 2)), deserialized.getStructuredAttributes().get("fluent"));
    }
}
