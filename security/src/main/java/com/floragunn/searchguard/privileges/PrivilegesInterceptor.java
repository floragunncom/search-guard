/*
 * Copyright 2015-2022 floragunn GmbH
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

package com.floragunn.searchguard.privileges;

import java.util.Map;

import org.opensearch.action.ActionRequest;

import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.authz.ActionAuthorization;
import com.floragunn.searchguard.authz.PrivilegesEvaluationContext;
import com.floragunn.searchguard.authz.PrivilegesEvaluationException;
import com.floragunn.searchguard.authz.actions.Action;
import com.floragunn.searchguard.user.User;

public interface PrivilegesInterceptor {

    InterceptionResult replaceKibanaIndex(
            PrivilegesEvaluationContext context, ActionRequest request, Action action, ActionAuthorization actionAuthorization) throws PrivilegesEvaluationException;

    Map<String, Boolean> mapTenants(User user, ImmutableSet<String> roles, ActionAuthorization actionAuthorization);

    boolean isEnabled();

    String getKibanaIndex();

    String getKibanaServerUser();

    enum InterceptionResult {
        ALLOW, DENY, NORMAL
    }
}
