/*
 * Copyright 2021-2022 floragunn GmbH
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

import java.util.HashMap;
import java.util.Map;

import com.floragunn.codova.config.templates.ExpressionEvaluationException;
import com.floragunn.codova.config.templates.Template;
import com.floragunn.codova.config.text.Pattern;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.authz.actions.Action;
import com.floragunn.searchguard.authz.actions.ActionRequestIntrospector;
import com.floragunn.searchguard.authz.actions.ActionRequestIntrospector.ActionRequestInfo;
import com.floragunn.searchguard.privileges.SpecialPrivilegesEvaluationContext;
import com.floragunn.searchguard.user.User;

/**
 * Request-scoped context information for privilege evaluation.
 * 
 * This class carries metadata about the request and provides caching facilities for data which might need to be evaluated several times per request.
 */
public class PrivilegesEvaluationContext {
    private boolean resolveLocalAll = true;
    private final User user;
    private final boolean userIsAdmin;
    private final Action action;
    private final Object request;
    private final Map<Template<Pattern>, Pattern> renderedPatternTemplateCache = new HashMap<>();
    private final ImmutableSet<String> mappedRoles;
    private final ActionRequestIntrospector actionRequestIntrospector;
    private final SpecialPrivilegesEvaluationContext specialPrivilegesEvaluationContext;
    private final boolean debugEnabled;
    private ActionRequestInfo requestInfo;

    public PrivilegesEvaluationContext(User user, boolean userIsAdmin, ImmutableSet<String> mappedRoles, Action action, Object request, boolean debugEnabled,
            ActionRequestIntrospector actionRequestIntrospector, SpecialPrivilegesEvaluationContext specialPrivilegesEvaluationContext) {
        this.user = user;
        this.userIsAdmin = userIsAdmin;
        this.mappedRoles = mappedRoles;
        this.action = action;
        this.request = request;
        this.actionRequestIntrospector = actionRequestIntrospector;
        this.debugEnabled = debugEnabled;
        this.specialPrivilegesEvaluationContext = specialPrivilegesEvaluationContext;
    }

    public User getUser() {
        return user;
    }
    
    public boolean isUserAdmin() {
        return this.userIsAdmin;
    }

    public boolean isResolveLocalAll() {
        return resolveLocalAll;
    }

    public void setResolveLocalAll(boolean resolveLocalAll) {
        this.resolveLocalAll = resolveLocalAll;
    }

    public Pattern getRenderedPattern(Template<Pattern> template) throws ExpressionEvaluationException {
        Pattern pattern = this.renderedPatternTemplateCache.get(template);

        if (pattern == null) {
            pattern = template.render(user);
            this.renderedPatternTemplateCache.put(template, pattern);
        }

        return pattern;
    }

    public ActionRequestInfo getRequestInfo() {
        if (this.requestInfo == null) {
            this.requestInfo = this.actionRequestIntrospector.getActionRequestInfo(this.action, this.request);
        }

        return this.requestInfo;
    }

    /**
     * The action that belongs to this request. May be null for evaluations which are independent of specific requests. 
     */
    public Action getAction() {
        return action;
    }

    /**
     * The request that triggered the index evaluation. May be null for evaluations which are independent of specific requests.
     */
    public Object getRequest() {
        return request;
    }

    public boolean isDebugEnabled() {
        return debugEnabled;
    }

    public ImmutableSet<String> getMappedRoles() {
        return mappedRoles;
    }

    public PrivilegesEvaluationContext mappedRoles(ImmutableSet<String> mappedRoles) {
        if (this.mappedRoles != null && this.mappedRoles.equals(mappedRoles)) {
            return this;
        } else {
            return new PrivilegesEvaluationContext(user, this.userIsAdmin, mappedRoles, action, mappedRoles, debugEnabled, actionRequestIntrospector,
                    specialPrivilegesEvaluationContext);
        }
    }

    public SpecialPrivilegesEvaluationContext getSpecialPrivilegesEvaluationContext() {
        return specialPrivilegesEvaluationContext;
    }
}
