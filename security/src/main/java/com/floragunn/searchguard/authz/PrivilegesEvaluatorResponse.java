/*
 * Copyright 2015-2018 floragunn GmbH
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.elasticsearch.action.support.ActionFilter;

import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.authz.actions.ActionRequestIntrospector.ActionRequestInfo;
import com.floragunn.searchguard.authz.actions.ResolvedIndices;

public class PrivilegesEvaluatorResponse {
    boolean allowed = false;
    Set<String> missingPrivileges = new HashSet<String>();
    List<ActionFilter> additionalActionFilters;
    ImmutableSet<String> mappedRoles;

    PrivilegesEvaluatorResponseState state = PrivilegesEvaluatorResponseState.PENDING;
    ActionRequestInfo actionRequestInfo;

    public ActionRequestInfo getActionRequestInfo() {
        return actionRequestInfo;
    }
    
    public ResolvedIndices getResolvedIndices() {
        return actionRequestInfo != null ? actionRequestInfo.getResolvedIndices() : null;
    }

    public boolean isAllowed() {
        return allowed;
    }

    public Set<String> getMissingPrivileges() {
        return new HashSet<String>(missingPrivileges);
    }

    public PrivilegesEvaluatorResponse markComplete() {
        this.state = PrivilegesEvaluatorResponseState.COMPLETE;
        return this;
    }

    public PrivilegesEvaluatorResponse markPending() {
        this.state = PrivilegesEvaluatorResponseState.PENDING;
        return this;
    }

    public boolean isComplete() {
        return this.state == PrivilegesEvaluatorResponseState.COMPLETE;
    }

    public boolean isPending() {
        return this.state == PrivilegesEvaluatorResponseState.PENDING;
    }

    @Override
    public String toString() {
        return "PrivEvalResponse [allowed=" + allowed + ", missingPrivileges=" + missingPrivileges + "]";
    }

    public static enum PrivilegesEvaluatorResponseState {
        PENDING, COMPLETE;
    }

    void addAdditionalActionFilter(ActionFilter actionFilter) {
        if (this.additionalActionFilters == null) {
            this.additionalActionFilters = new ArrayList<>(4);
        }

        this.additionalActionFilters.add(actionFilter);
    }
    
    public boolean hasAdditionalActionFilters() {
        return additionalActionFilters != null && additionalActionFilters.size() > 0;
    }

    public List<ActionFilter> getAdditionalActionFilters() {
        if (additionalActionFilters != null) {
            return additionalActionFilters;
        } else {
            return Collections.emptyList();
        }
    }

    public ImmutableSet<String> getMappedRoles() {
        return mappedRoles;
    }

}