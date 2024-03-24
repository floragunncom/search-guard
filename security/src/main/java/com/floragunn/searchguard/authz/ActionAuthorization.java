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

import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.authz.actions.Action;
import com.floragunn.searchguard.authz.actions.ResolvedIndices;

/**
 * Common interface for checking authorization for actions.
 *
 * API considerations: 
 * 
 * All methods carry a context parameter. This object carries request-scoped metadata about the user. 
 * Additionally, the methods carry more parameters which specify the resources to be verified.  
 * These parameters may deviate from request-scoped metadata, as the methods might be use to check certain 
 * facets which are only indirectly related to the index. Example: The action parameter may deviate from the action
 * referenced in the context, because additional privileges need to be checked for proper authorization.
 */
public interface ActionAuthorization {
    PrivilegesEvaluationResult hasClusterPermission(PrivilegesEvaluationContext context, Action action) throws PrivilegesEvaluationException;
    
    PrivilegesEvaluationResult hasIndexPermission(PrivilegesEvaluationContext context, Action primaryAction, ImmutableSet<Action> actions,
            ResolvedIndices resolvedIndices, Action.Scope scope) throws PrivilegesEvaluationException;

    PrivilegesEvaluationResult hasTenantPermission(PrivilegesEvaluationContext context, Action action, String requestedTenant)
            throws PrivilegesEvaluationException;

    default PrivilegesEvaluationResult hasIndexPermission(PrivilegesEvaluationContext context, Action primaryAction, ImmutableSet<Action> actions,
            ResolvedIndices resolvedIndices) throws PrivilegesEvaluationException {
        return hasIndexPermission(context, primaryAction, actions, resolvedIndices, primaryAction.scope());
    }
    
    static enum AliasDataStreamHandling {
        RESOLVE_IF_NECESSARY, DO_NOT_RESOLVE
    }
}
