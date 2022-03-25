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

package com.floragunn.searchguard.authz;

import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.authz.Action.WellKnownAction;
import com.floragunn.searchguard.sgconf.ActionGroups;
import com.floragunn.searchguard.sgconf.impl.CType;
import com.floragunn.searchguard.sgconf.impl.SgDynamicConfiguration;
import com.floragunn.searchguard.user.User;

public class RoleBasedActionAuthorizationTests {

    private static final ActionGroups emptyActionGroups = new ActionGroups(SgDynamicConfiguration.empty());
    private static final Actions actions = new Actions(null);

    @Test
    public void clusterAction_wellKnown() throws Exception {

        Action nodesStatsAction = actions.get("cluster:monitor/nodes/stats");
        Action otherAction = actions.get("cluster:monitor/nodes/usage");

        Assert.assertTrue(nodesStatsAction.toString(), nodesStatsAction instanceof WellKnownAction);

        SgDynamicConfiguration<Role> roles = SgDynamicConfiguration.fromMap(
                DocNode.of("test_role", DocNode.of("cluster_permissions", Arrays.asList("cluster:monitor/nodes/stats*"))), CType.ROLES, -1, -1, -1,
                null);

        ImmutableSet<String> tenants = ImmutableSet.empty();

        RoleBasedActionAuthorization subject = new RoleBasedActionAuthorization(roles, emptyActionGroups, actions, null, tenants);

        User user = User.forUser("test").build();

        Assert.assertTrue(subject.hasClusterPermission(user, ImmutableSet.of("test_role"), nodesStatsAction));
        Assert.assertFalse(subject.hasClusterPermission(user, ImmutableSet.of("other_role"), nodesStatsAction));
        Assert.assertFalse(subject.hasClusterPermission(user, ImmutableSet.of("test_role"), otherAction));
    }
    
    @Test
    public void clusterAction_notWellKnown() throws Exception {

        Action nodesStatsAction = actions.get("cluster:monitor/nodes/stats/somethingnotwellknown");
        Action otherAction = actions.get("cluster:monitor/nodes/usage/somethingnotwellknown");

        Assert.assertFalse(nodesStatsAction.toString(), nodesStatsAction instanceof WellKnownAction);

        SgDynamicConfiguration<Role> roles = SgDynamicConfiguration.fromMap(
                DocNode.of("test_role", DocNode.of("cluster_permissions", Arrays.asList("cluster:monitor/nodes/stats*"))), CType.ROLES, -1, -1, -1,
                null);

        ImmutableSet<String> tenants = ImmutableSet.empty();

        RoleBasedActionAuthorization subject = new RoleBasedActionAuthorization(roles, emptyActionGroups, actions, null, tenants);

        User user = User.forUser("test").build();

        Assert.assertTrue(subject.hasClusterPermission(user, ImmutableSet.of("test_role"), nodesStatsAction));
        Assert.assertFalse(subject.hasClusterPermission(user, ImmutableSet.of("other_role"), nodesStatsAction));
        Assert.assertFalse(subject.hasClusterPermission(user, ImmutableSet.of("test_role"), otherAction));
    }

}
