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

package com.floragunn.searchguard.authz.actions;

import com.floragunn.searchguard.authz.PrivilegesEvaluationContext;
import com.floragunn.searchguard.authz.PrivilegesEvaluationException;
import com.floragunn.searchguard.authz.PrivilegesEvaluationResult;
import com.floragunn.searchguard.authz.actions.ActionRequestIntrospector.ResolvedIndices;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.user.User;

public interface ActionAuthorization {
    boolean hasClusterPermission(User user, ImmutableSet<String> mappedRoles, Action action) throws PrivilegesEvaluationException;

    PrivilegesEvaluationResult hasIndexPermission(User user, ImmutableSet<String> mappedRoles, ImmutableSet<Action> actions,
            ResolvedIndices resolvedIndices, PrivilegesEvaluationContext context) throws PrivilegesEvaluationException;

    boolean hasTenantPermission(User user, String requestedTenant, ImmutableSet<String> mappedRoles, Action action,
            PrivilegesEvaluationContext context) throws PrivilegesEvaluationException;

}
