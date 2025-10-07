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
package com.floragunn.searchguard.authz;

import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Format;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.authz.config.ActionGroup;
import com.floragunn.searchguard.configuration.CType;
import com.floragunn.searchguard.configuration.SgDynamicConfiguration;

public class ActionGroupsTest {

    @Test
    public void basicTest() throws Exception {
        TestActionGroup testActionGroups = new TestActionGroup().with("Z", "C", "A").with("A", "A1", "A2", "A3").with("B", "B1", "B2", "B3").with("C",
                "A", "B", "C1");
        SgDynamicConfiguration<ActionGroup> config = SgDynamicConfiguration.fromMap(testActionGroups.map, CType.ACTIONGROUPS, null).get();

        ActionGroup.FlattenedIndex actionGroups = new ActionGroup.FlattenedIndex(config);

        Assert.assertEquals(ImmutableSet.of("C", "A", "A1", "A2", "A3", "C1", "B", "B1", "B2", "B3", "Z"),
                actionGroups.resolve(ImmutableSet.of("Z")));

        Assert.assertEquals(ImmutableSet.of("A", "A1", "A2", "A3"), actionGroups.resolve(ImmutableSet.of("A")));
    }

    @Test
    public void recursionTest() throws Exception {
        TestActionGroup testActionGroups = new TestActionGroup().with("A", "A1", "B").with("B", "B1", "C").with("C", "C1", "A", "D").with("D", "D1");
        SgDynamicConfiguration<ActionGroup> config = SgDynamicConfiguration.fromMap(testActionGroups.map, CType.ACTIONGROUPS, null).get();

        ActionGroup.FlattenedIndex actionGroups = new ActionGroup.FlattenedIndex(config);

        Assert.assertEquals(ImmutableSet.of("A", "A1", "B", "B1", "C", "C1", "D", "D1"), actionGroups.resolve(ImmutableSet.of("A")));
        Assert.assertEquals(ImmutableSet.of("A", "A1", "B", "B1", "C", "C1", "D", "D1"), actionGroups.resolve(ImmutableSet.of("C")));
        Assert.assertEquals(ImmutableSet.of("D", "D1"), actionGroups.resolve(ImmutableSet.of("D")));

    }

    @Test
    public void staticActionGroupsSmokeTest() throws Exception {
        SgDynamicConfiguration<ActionGroup> config = SgDynamicConfiguration.fromDocNode(
                DocNode.parse(Format.YAML).from(new InputStreamReader(getClass().getResourceAsStream("/static_config/static_action_groups.yml"))),
                null, CType.ACTIONGROUPS, 0l, 0l, 0l, null).get();

        ActionGroup.FlattenedIndex actionGroups = new ActionGroup.FlattenedIndex(config);

        ImmutableSet<String> resolved = actionGroups.resolve(ImmutableSet.of("SGS_CRUD"));
        Assert.assertTrue(resolved.toString(), resolved.contains("SGS_READ"));
        Assert.assertTrue(resolved.toString(), resolved.contains("indices:data/read*"));
        Assert.assertTrue(resolved.toString(), resolved.contains("SGS_CRUD"));
    }

    private static class TestActionGroup {
        private Map<String, Object> map = new HashMap<>();

        TestActionGroup with(String key, String... actions) {
            map.put(key, ImmutableMap.of("allowed_actions", Arrays.asList(actions), "type", "index"));
            return this;
        }
    }

}
